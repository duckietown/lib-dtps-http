#[cfg(target_os = "linux")]
use getaddrs::InterfaceAddrs;
use log::debug;

#[cfg(target_os = "linux")]
pub fn get_other_addresses() -> Vec<String> {
    println!("You are running Linux!");

    let addrs = InterfaceAddrs::query_system().expect("System has no network interfaces.");
    // debug!("Found {} network interfaces", addrs.len());
    let mut ret = Vec::new();
    ret.push("localhost".to_string());
    for addr in addrs {
        if let Some(ipv4_addr) = addr.address {
            debug!("{}: {:?}", addr.name, ipv4_addr);
            ret.push(ipv4_addr.to_string());
        }
        // if let Some(addr2) = addr.ipv6() {
        //     debug!("{}: {:?}", addr.name, addr2);
        //     ret.push(addr2.to_string());
        // }

        debug!("{}: {:?}", addr.name, addr.address);
    }
    debug!("You are running Linux - using other addresses");
    ret
}

#[cfg(not(target_os = "linux"))]
pub fn get_other_addresses() -> Vec<String> {
    debug!("You are not running Linux - ignoring other addresses");
    let mut ret = Vec::new();
    ret.push("localhost".to_string());
    ret
}
