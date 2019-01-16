use std::collections::HashMap;
pub trait RedisCommand {
    fn on_cmd_ping(&self,key: &[u8]);
    fn on_cmd_echo(&self,val: &[u8]);
    fn on_cmd_quit(&self)->Result<(),String>;

    fn on_cmd_auth(&self,password: &[u8])->Result<(), String>;
    fn on_cmd_flushdb(&self)->Result<(),String>;
    fn on_cmd_keys(&self)->Result<Vec<Vec<u8>>,String>;


    fn on_cmd_del(&self,key: &[u8])->Result<(),String>;
    fn on_cmd_get(&self,key: &[u8])->Result<Vec<u8>,String>;
    fn on_cmd_hmget(&self,hash: &[u8], keys: &[Vec<u8>])->Result<Vec<Vec<u8>>,String>;
    fn on_cmd_hget(&self,hash: &[u8], key: &[u8])->Result<Vec<u8>,String>;
    fn on_cmd_hgetall(&self,hash: &[u8])->Result<HashMap<String, String>,String>;
    fn on_cmd_set(&self,key: &[u8], val: &[u8])->Result<(),String>;
    fn on_cmd_hmset(&self,hash: &[u8], kv: &HashMap<&Vec<u8>, &Vec<u8>>)->Result<(),String>;
    fn on_cmd_hset(&self,hash: &[u8],  key: &[u8], val: &[u8])->Result<(),String>;

    fn on_cmd_msg(&self,val: &[u8])->Result<(),String>;
    fn on_cmd_subscribe(&self,val: &[u8])->Result<(),String>;
    fn on_cmd_cluster_nodes(&self)->Result<Vec<u8>,String>;
    fn on_cmd_cluster_slots(&self)->Result<Vec<u8>,String>;
}