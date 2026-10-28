//! Temporary power-profile holds. No governor writes, saved-profile files,
//! privileged helpers, background threads, or work when disabled/idle.
use serde_json::{json, Value};

#[derive(Default)]
pub struct PowerProfile {
    requested: bool,
    state: &'static str,
    error: Option<String>,
    #[cfg(target_os = "linux")]
    hold: Option<linux::Hold>,
}

impl PowerProfile {
    /// A burst starts only when a job can actually launch; queued jobs alone
    /// never change the profile. Keep the hold until cleanup releases resources.
    pub fn update(&mut self, enabled: bool, busy: bool) {
        if !enabled || !busy {
            #[cfg(target_os = "linux")]
            {
                self.hold = None;
            } // Disconnect releases only our hold, even after SIGKILL.
            self.requested = false;
            self.state = if enabled { "idle" } else { "disabled" };
            return;
        }
        if self.requested {
            return; // Do not fight manual changes or repeatedly retry a denial.
        }
        self.requested = true;
        self.error = None;
        #[cfg(target_os = "linux")]
        match linux::Hold::acquire() {
            Ok(hold) => {
                self.hold = Some(hold);
                self.state = "held";
            }
            Err(e) => {
                self.state = "unavailable";
                self.error = Some(format!("{e:#}"));
                eprintln!("warning: performance profile unavailable; jobs continue: {e:#}");
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.state = "unsupported";
        }
    }

    pub fn poll(&mut self) {
        #[cfg(target_os = "linux")]
        if let Some(hold) = self.hold.as_mut() {
            match hold.poll() {
                Ok(true) => {
                    self.state = "released_externally";
                    self.hold = None;
                }
                Ok(false) => {}
                Err(e) => {
                    self.state = "unavailable";
                    self.error = Some(format!("{e:#}"));
                    self.hold = None;
                }
            }
        }
    }

    pub fn pollfd(&self) -> Option<libc::pollfd> {
        #[cfg(target_os = "linux")]
        if let Some(hold) = &self.hold {
            let watch = hold.connection.channel().watch();
            return Some(libc::pollfd {
                fd: watch.fd,
                events: (if watch.read { libc::POLLIN } else { 0 })
                    | (if watch.write { libc::POLLOUT } else { 0 }),
                revents: 0,
            });
        }
        None
    }

    pub fn status(&self, enabled: bool) -> Value {
        #[allow(unused_mut)]
        let mut value = json!({"enabled":enabled,
            "state":if self.state.is_empty() { if enabled {"idle"} else {"disabled"} } else {self.state},
            "requested_profile":"performance", "last_error":self.error});
        #[cfg(target_os = "linux")]
        if let Some(hold) = &self.hold {
            value["observed_profile"] = json!(hold.active);
            value["performance_degraded"] = json!(hold.degraded);
        }
        value
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use anyhow::{bail, Context, Result};
    use dbus::{
        arg::PropMap,
        blocking::{stdintf::org_freedesktop_dbus::Properties, Connection},
        channel::{BusType, Channel},
        MessageType,
    };
    use std::time::Duration;

    const TIMEOUT: Duration = Duration::from_millis(500);
    const BUS: &str = "org.freedesktop.DBus";
    const PROPS: &str = "org.freedesktop.DBus.Properties";

    pub struct Hold {
        pub connection: Connection,
        owner: String,
        interface: &'static str,
        cookie: u32,
        pub active: String,
        pub degraded: String,
    }

    impl Hold {
        pub fn acquire() -> Result<Self> {
            let mut channel = Channel::get_private(BusType::System)
                .context("connect to system D-Bus (power-profiles-daemon required)")?;
            channel.set_watch_enabled(true);
            let connection = Connection::from(channel);
            let bus = connection.with_proxy(BUS, "/org/freedesktop/DBus", TIMEOUT);
            let endpoints = [
                (
                    "org.freedesktop.UPower.PowerProfiles",
                    "/org/freedesktop/UPower/PowerProfiles",
                ),
                ("net.hadess.PowerProfiles", "/net/hadess/PowerProfiles"),
            ];
            let mut endpoint = None;
            for (name, path) in endpoints {
                let owner: Result<(String,), _> = bus.method_call(BUS, "GetNameOwner", (name,));
                if let Ok((owner,)) = owner {
                    endpoint = Some((name, path, owner));
                    break;
                }
            }
            let (interface, path, owner) =
                endpoint.context("power-profiles-daemon is not running")?;
            connection.add_match_no_cb(&format!(
                "type='signal',sender='{BUS}',interface='{BUS}',member='NameOwnerChanged',arg0='{interface}'"))?;
            connection.add_match_no_cb(&format!(
                "type='signal',sender='{owner}',path='{path}',interface='{PROPS}',member='PropertiesChanged'"))?;
            let proxy = connection.with_proxy(&owner, path, TIMEOUT);
            let (cookie,): (u32,) = proxy
                .method_call(
                    interface,
                    "HoldProfile",
                    ("performance", "Running queued jobs", "org.jobq.Jobq"),
                )
                .context(
                    "HoldProfile denied or unavailable; authorize the hold-profile Polkit action",
                )?;
            // A power-saver hold or thermal constraints may take precedence.
            let active: String = proxy.get(interface, "ActiveProfile")?;
            let degraded: String = proxy
                .get(interface, "PerformanceDegraded")
                .unwrap_or_default();
            Ok(Self {
                connection,
                owner,
                interface,
                cookie,
                active,
                degraded,
            })
        }

        /// Drain a bounded number of signals with zero timeout. The server polls
        /// this connection's fd alongside existing sockets; no extra timer.
        pub fn poll(&mut self) -> Result<bool> {
            if self
                .connection
                .channel()
                .read_write(Some(Duration::ZERO))
                .is_err()
            {
                bail!("system D-Bus disconnected; performance hold released");
            }
            for _ in 0..32 {
                let Some(message) = self.connection.channel().pop_message() else {
                    break;
                };
                if message.msg_type() != MessageType::Signal {
                    continue;
                }
                let sender = message.sender().map(|s| s.to_string()).unwrap_or_default();
                let member = message.member().map(|s| s.to_string()).unwrap_or_default();
                if sender == BUS && member == "NameOwnerChanged" {
                    if let Ok((name, old, new)) = message.read3::<String, String, String>() {
                        if name == self.interface && old == self.owner && new != self.owner {
                            bail!("power-profile service restarted; retry on next busy period");
                        }
                    }
                }
                if sender != self.owner {
                    continue;
                }
                if member == "ProfileReleased" && message.read1::<u32>().ok() == Some(self.cookie) {
                    return Ok(true); // Respect manual selection until the next busy period.
                }
                if member == "PropertiesChanged" {
                    if let Ok((interface, changed, _)) =
                        message.read3::<String, PropMap, Vec<String>>()
                    {
                        if interface == self.interface {
                            if let Some(s) = changed.get("ActiveProfile").and_then(|v| v.0.as_str())
                            {
                                self.active = s.into();
                            }
                            if let Some(s) = changed
                                .get("PerformanceDegraded")
                                .and_then(|v| v.0.as_str())
                            {
                                self.degraded = s.into();
                            }
                        }
                    }
                }
            }
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_and_idle_do_not_connect_or_change_profiles() {
        let mut power = PowerProfile::default();
        power.update(false, true);
        assert_eq!(power.status(false)["state"], "disabled");
        power.update(true, false);
        assert_eq!(power.status(true)["state"], "idle");
        assert!(power.pollfd().is_none());
        assert!(!crate::config::FileConfig::default().performance_while_running);
        let cfg: crate::config::FileConfig =
            toml::from_str("performance_while_running = true").unwrap();
        assert!(
            crate::config::Config::resolve(cfg, 2, 8 << 30)
                .unwrap()
                .performance_while_running
        );
    }
}
