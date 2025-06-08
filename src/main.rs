use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::net::Ipv4Addr;
use maxminddb::{MaxMindDBError, Reader, Within};
use serde::{Deserialize, Serialize};
use indicatif::{ProgressBar, ProgressStyle};
use ipnetwork::IpNetwork;
use std::str::FromStr;

#[derive(Deserialize)]
struct CountryRecord {
    country: Option<Country>,
}

#[derive(Deserialize)]
struct Country {
    iso_code: Option<String>,
}

#[derive(Serialize)]
struct Output {
    foreign: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct NetworkBlock {
    network: u32,
    prefix_len: u8,
}

impl NetworkBlock {
    fn new(ip: u32, prefix_len: u8) -> Self {
        let mask = if prefix_len == 0 { 0 } else { !((1u32 << (32 - prefix_len)) - 1) };
        let network = ip & mask;
        NetworkBlock { network, prefix_len }
    }

    fn to_string(&self) -> String {
        let ip = Ipv4Addr::from(self.network);
        format!("{}/{}", ip, self.prefix_len)
    }

    fn contains(&self, other: &NetworkBlock) -> bool {
        if self.prefix_len >= other.prefix_len {
            return false;
        }
        let mask = if self.prefix_len == 0 { 0 } else { !((1u32 << (32 - self.prefix_len)) - 1) };
        (self.network & mask) == (other.network & mask)
    }
    fn last(&self) -> u32 {
        let mask = if self.prefix_len == 0 { 0 } else { !((1u32 << (32 - self.prefix_len)) - 1) };
        let last = (self.network & mask) + !mask;
        last
    }
}

fn ip_to_u32(ip: Ipv4Addr) -> u32 {
    u32::from(ip)
}

fn mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        (!0u32) << (32 - prefix)
    }
}

fn block_size(prefix: u8) -> u32 {
    1u32 << (32 - prefix)
}

fn try_merge(a: &NetworkBlock, b: &NetworkBlock) -> Option<NetworkBlock> {
    if a.prefix_len == b.prefix_len && a.last() + 1 == b.network {
        let range_size = block_size(a.prefix_len) + block_size(b.prefix_len);
        let prefix = 32 - range_size.trailing_zeros() as u8;
        Some(NetworkBlock::new(a.network, prefix))
    } else {
        None
    }
}

//#[test]
fn try_marge_test(){
    let block1 = NetworkBlock::new(ip_to_u32(Ipv4Addr::from_str("1.0.1.0").unwrap()), 24);
    let block2 = NetworkBlock::new(ip_to_u32(Ipv4Addr::from_str("1.0.2.0").unwrap()), 23);
    let result = try_merge(&block1, &block2);
    assert!(result.is_some());
}

#[test]
fn test_unknown_country() {
    let reader = Reader::open_readfile("GeoLite2-Country.mmdb");
    let binding = reader.expect("aaaaa");
    let mut iter: Within<CountryRecord, _> = binding.within(IpNetwork::V4("1.0.164.22/32".parse().unwrap())).unwrap();
    while let Some(result) = iter.next() {
        match result {
            Ok(item) => {
                if let Some(country) = item.info.country {
                    println!("{}", country.iso_code.unwrap())
                } else {
                    println!("None")
                }
            }
            Err(_) => {}
        }
    }
    println!("end")
}

fn optimize_blocks_simple(blocks: Vec<NetworkBlock>) -> Vec<NetworkBlock> {
    if blocks.len() <= 1 {
        return blocks;
    }

    println!("最適化開始: {} ブロック", blocks.len());
    let mut sorted_blocks = blocks;
    sorted_blocks.sort_by(|a, b| {
        a.network.cmp(&b.network).then(a.prefix_len.cmp(&b.prefix_len))
    });
    println!("ソート完了");

    let mut processed = 0;
    let total = sorted_blocks.len();
    
    let mut result: Vec<NetworkBlock> = Vec::new();

    for mut blk in sorted_blocks {
        if let Some(top) = result.last() {
            if top.contains(&blk) {
                continue;
            }
        }

        result.push(blk);
        loop {
            if result.len() < 2 {
                break;
            }
            let len = result.len();
            let b = result[len - 1].clone();
            let a = result[len - 2].clone();

            if let Some(parent) = try_merge(&a, &b) {
                result.pop();
                result.pop();

                if let Some(prev) = result.last() {
                    if prev.contains(&parent) {
                        continue;
                    }
                }
                blk = parent.clone();
                result.push(parent);
            } else {
                break;
            }
        }
    }

    println!("最適化完了: {} ブロック → {} ブロック", total, result.len());
    result
}

fn process_geolite2_networks(db_path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    println!("GeoLite2データベースを読み込み中...");
    let reader = Reader::open_readfile(db_path)?;
    
    println!("ネットワーク情報を取得中...");
    
    let mut foreign_blocks = HashSet::new();
    let mut total_networks = 0;
    let mut japan_networks = 0;
    
    let mut iter: Within<CountryRecord, _> = reader.within(IpNetwork::V4("0.0.0.0/0".parse().unwrap())).unwrap();

    while let Some(result) = iter.next() {
        match result {
            Ok(item) => {
                total_networks += 1;
                //if total_networks > 10 {
                //    break;
                //}

                if let Some(country) = item.info.country {
                    let is_japan = country.iso_code
                        .map(|code| code == "JP")
                        .unwrap_or(false);
                    
                    //println!("is_japan: {}, network: {}/{}", is_japan, item.ip_net.ip(), item.ip_net.prefix());

                    if is_japan {
                        japan_networks += 1;
                    } else {
                        let ip_u32 = ip_to_u32(match item.ip_net.ip() {
                            std::net::IpAddr::V4(ip) => ip,
                            _ => unreachable!("IPv6 is not supported"),
                        });
                        let block = NetworkBlock::new(ip_u32, item.ip_net.prefix());
                        foreign_blocks.insert(block);
                    }
                } else {
                    let ip_u32 = ip_to_u32(match item.ip_net.ip() {
                            std::net::IpAddr::V4(ip) => ip,
                            _ => unreachable!("IPv6 is not supported"),
                        });
                        let block = NetworkBlock::new(ip_u32, item.ip_net.prefix());
                        foreign_blocks.insert(block);
                }
            }
            Err(_) => continue,
        }

        if total_networks % 1000 == 0 {
            print!("\r処理済み: {} ネットワーク (日本: {})", total_networks, japan_networks);
            std::io::stdout().flush().unwrap();
        }
    }
    
    println!("\n\nネットワーク処理完了:");
    println!("  総ネットワーク数: {}", total_networks);
    println!("  日本のネットワーク: {}", japan_networks);
    println!("  海外のネットワーク: {}", foreign_blocks.len());
    
    println!("\nCIDR最適化中...");
    let blocks_vec: Vec<NetworkBlock> = foreign_blocks.into_iter().collect();
    println!("最適化開始: {} ブロック", blocks_vec.len());
    let optimized_blocks = optimize_blocks_simple(blocks_vec.clone());
    
    println!("最適化完了: {} -> {} ブロック", blocks_vec.len(), optimized_blocks.len());
    
    let mut result: Vec<String> = optimized_blocks.iter()
        .map(|block| block.to_string())
        .collect();
    
    result.sort_by(|a, b| {
        let parse_ip = |s: &str| -> (u32, u8) {
            let parts: Vec<&str> = s.split('/').collect();
            let ip_parts: Vec<u32> = parts[0].split('.').map(|x| x.parse().unwrap()).collect();
            let ip = (ip_parts[0] << 24) | (ip_parts[1] << 16) | (ip_parts[2] << 8) | ip_parts[3];
            let prefix: u8 = parts[1].parse().unwrap();
            (ip, prefix)
        };
        
        let (ip_a, prefix_a) = parse_ip(a);
        let (ip_b, prefix_b) = parse_ip(b);
        ip_a.cmp(&ip_b).then(prefix_a.cmp(&prefix_b))
    });
    
    Ok(result)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "GeoLite2-Country.mmdb";
    
    println!("=== 海外IP CIDR生成ツール ===");
    println!("対象データベース: {}", db_path);
    
    let start_time = std::time::Instant::now();
    
    match process_geolite2_networks(db_path) {
        Ok(foreign_cidrs) => {
            let output = Output {
                foreign: foreign_cidrs,
            };
            
            println!("\nJSONファイル出力中...");
            let json_output = serde_json::to_string_pretty(&output)?;
            let mut file = File::create("foreign_ip_cidrs.json")?;
            file.write_all(json_output.as_bytes())?;
            
            let elapsed = start_time.elapsed();
            
            println!("\n=== 処理完了 ===");
            println!("出力ファイル: foreign_ip_cidrs.json");
            println!("CIDR数: {}", output.foreign.len());
            println!("処理時間: {:.2}秒", elapsed.as_secs_f64());
            println!("ファイルサイズ: {:.2} KB", json_output.len() as f64 / 1024.0);
            
            if !output.foreign.is_empty() {
                println!("\n=== サンプル (最初の50件) ===");
                for (i, cidr) in output.foreign.iter().take(50).enumerate() {
                    println!("{:2}: {}", i + 1, cidr);
                }
                if output.foreign.len() > 50 {
                    println!("... (残り{}件)", output.foreign.len() - 50);
                }
                
                let prefix_counts = output.foreign.iter().fold(std::collections::HashMap::new(), |mut acc, cidr| {
                    let prefix = cidr.split('/').nth(1).unwrap();
                    *acc.entry(prefix.to_string()).or_insert(0) += 1;
                    acc
                });
                
                println!("\n=== プレフィックス長別統計 ===");
                let mut sorted_prefixes: Vec<_> = prefix_counts.iter().collect();
                sorted_prefixes.sort_by_key(|(prefix, _)| prefix.parse::<u8>().unwrap_or(0));
                
                for (prefix, count) in sorted_prefixes {
                    println!("/{}: {} ブロック", prefix, count);
                }
            }
        }
        Err(e) => {
            eprintln!("エラー: {}", e);
            eprintln!("ファイル '{}' が存在することを確認してください。", db_path);
            std::process::exit(1);
        }
    }
    
    Ok(())
}
