use hashbrown::HashMap;
pub trait RedisCommand {
    fn on_cmd_ping(&self, key: &[u8]);
    fn on_cmd_echo(&self, val: &[u8]);
    fn on_cmd_quit(&self) -> Result<(), String>;

    fn on_cmd_auth(&self, db: &[u8], password: &[u8]) -> Result<(), String>;
    fn on_cmd_flushdb(&self, db: &[u8]) -> Result<(), String>;
    fn on_cmd_keys(&self, db: &[u8]) -> Result<Vec<Vec<u8>>, String>;

    fn on_cmd_backupdb(&self, db: &[u8]) -> Result<(), String>;
    fn on_cmd_backup_lru_keys(&self, db: &[u8]) -> Result<(), String>;

    fn on_cmd_del(&self, db: &[u8], key: &[u8]) -> Result<(), String>;
    fn on_cmd_get(&self, db: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String>;
    fn on_cmd_hmget(
        &self,
        db: &[u8],
        hash: &[u8],
        keys: &[Vec<u8>],
    ) -> Result<Option<Vec<Vec<u8>>>, String>;
    fn on_cmd_hget(&self, db: &[u8], hash: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String>;
    fn on_cmd_hgetall(&self, db: &[u8], hash: &[u8]) -> Result<Option<HashMap<Vec<u8>, Vec<u8>>>, String>;
    fn on_cmd_set(&self, db: &[u8], key: &[u8], val: &[u8]) -> Result<(), String>;
    fn on_cmd_hmset(
        &self,
        db: &[u8],
        hash: &[u8],
        kv: &HashMap<&Vec<u8>, &Vec<u8>>,
    ) -> Result<(), String>;
    fn on_cmd_hset(&self, db: &[u8], hash: &[u8], key: &[u8], val: &[u8]) -> Result<(), String>;

    fn on_cmd_msg(&self, channel: &[u8], val: &[u8]) -> Result<(), String>;
    fn on_cmd_subscribe(&self, val: &[u8]) -> Result<(), String>;
    fn on_cmd_subscribe_response(&self, channel: &[u8], status: &[u8]) -> Result<(), String>;
    fn on_cmd_cluster_nodes(&self) -> Result<Vec<u8>, String>;
    fn on_cmd_cluster_slots(&self) -> Result<Vec<u8>, String>;
    fn on_cmd_cluster_add_node(&self, kv: &HashMap<&Vec<u8>, &Vec<u8>>) -> Result<(), String>;
    fn on_cmd_cluster_update_node(&self, kv: &HashMap<&Vec<u8>, &Vec<u8>>) -> Result<(), String>;
    fn on_cmd_cluster_remove_node(&self, id: &[u8]) -> Result<(), String>;
}
