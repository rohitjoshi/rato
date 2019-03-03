#[macro_use]
extern crate log;

pub mod rato;
pub mod redis_cmd;
pub mod util;

#[cfg(test)]
mod tests {

    use super::*;
    use crate::rato::Rato;
    use crate::redis_cmd::RedisCommand;

    use hashbrown::HashMap;
    use std::sync::Arc;

    extern crate env_logger;

    struct RedisTest {}

    impl RedisCommand for RedisTest {
        fn on_cmd_ping(&self, key: &[u8]) {
            if !key.is_empty() {
                let test_key = b"test_key";
                assert_eq!(key, test_key);
            }
        }
        fn on_cmd_echo(&self, val: &[u8]) {}
        fn on_cmd_quit(&self) -> Result<(), String> {
            Err("Not implemented".to_string())
        }

        fn on_cmd_auth(&self, db: &[u8], password: &[u8]) -> Result<(), String> {
            if !password.is_empty() && password == b"secret" {
                return Ok(());
            }
            Err("Invalid password".to_string())
        }
        fn on_cmd_flushdb(&self, db: &[u8]) -> Result<(), String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_keys(&self, db: &[u8]) -> Result<Vec<Vec<u8>>, String> {
            Err("Not implemented".to_string())
        }

        fn on_cmd_del(&self, db: &[u8], key: &[u8]) -> Result<(), String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_get(&self, db: &[u8], key: &[u8]) -> Result<Vec<u8>, String> {
            Ok(b"b".to_vec())
        }
        fn on_cmd_hmget(
            &self,
            db: &[u8],
            hash: &[u8],
            keys: &[Vec<u8>],
        ) -> Result<Vec<Vec<u8>>, String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_hget(&self, db: &[u8], hash: &[u8], key: &[u8]) -> Result<Vec<u8>, String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_hgetall(
            &self,
            db: &[u8],
            hash: &[u8],
        ) -> Result<HashMap<Vec<u8>, Vec<u8>>, String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_set(&self, db: &[u8], key: &[u8], val: &[u8]) -> Result<(), String> {
            Ok(())
        }
        fn on_cmd_hmset(
            &self,
            db: &[u8],
            hash: &[u8],
            kv: &HashMap<&Vec<u8>, &Vec<u8>>,
        ) -> Result<(), String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_hset(
            &self,
            db: &[u8],
            hash: &[u8],
            key: &[u8],
            val: &[u8],
        ) -> Result<(), String> {
            Err("Not implemented".to_string())
        }

        fn on_cmd_msg(&self, channel: &[u8], val: &[u8]) -> Result<(), String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_subscribe(&self, val: &[u8]) -> Result<(), String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_subscribe_response(&self, channel: &[u8], status: &[u8]) -> Result<(), String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_cluster_nodes(&self) -> Result<Vec<u8>, String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_cluster_slots(&self) -> Result<Vec<u8>, String> {
            Err("Not implemented".to_string())
        }

        fn on_cmd_cluster_add_node(&self, kv: &HashMap<&Vec<u8>, &Vec<u8>>) -> Result<(), String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_cluster_remove_node(&self, id: &[u8]) -> Result<(), String> {
            Err("Not implemented".to_string())
        }

        #[inline]
        fn on_cmd_cluster_update_node(
            &self,
            kv: &HashMap<&Vec<u8>, &Vec<u8>>,
        ) -> Result<(), String> {
            Err("Not implemented".to_string())
        }

        fn on_cmd_backupdb(&self, db: &[u8]) -> Result<(), String> {
            Err("Not implemented".to_string())
        }
        fn on_cmd_backup_lru_keys(&self, table_name: &[u8]) -> Result<(), String> {
            Err("Not implemented".to_string())
        }
    }

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn on_cmd_auth_test() {
        let _ = env_logger::try_init();

        let redis_test = RedisTest {};
        //let redis_test= Arc::new(redis_test);

        //let mut mulo = Rato::<RedisTest>::new(redis_test);

        let mut tags: HashMap<String, String> = HashMap::with_capacity(1);
        tags.insert(Rato::DB_TAG.to_string(), "pan".to_string());

        let res = Rato::parse_input(&redis_test, &mut tags, &mut b"AUTH secret\r\n".to_vec());
        assert_eq!(String::from_utf8_lossy(&res.0), "+OK\r\n");

        let res = Rato::parse_input(&redis_test, &mut tags, &mut b"AUTH abc\r\n".to_vec());
        assert_ne!(String::from_utf8_lossy(&res.0), "+OK\r\n");
    }

    #[test]
    fn on_cmd_ping_test() {
        let _ = env_logger::try_init();

        let redis_test = RedisTest {};
        //let redis_test= Arc::new(redis_test);

        let mut tags: HashMap<String, String> = HashMap::with_capacity(1);
        tags.insert(Rato::DB_TAG.to_string(), "pan".to_string());
        tags.insert(Rato::AUTH_TAG.to_string(), "true".to_string());

        //let mut mulo = Rato::<RedisTest>::new(redis_test);

        let res = Rato::parse_input(&redis_test, &mut tags, &mut b"PING\r\n".to_vec());
        assert_eq!(String::from_utf8_lossy(&res.0), "+PONG\r\n");

        let res = Rato::parse_input(&redis_test, &mut tags, &mut b"PING test_key\r\n".to_vec());
        assert_eq!(String::from_utf8_lossy(&res.0), "$8\r\ntest_key\r\n");
    }

    #[test]
    fn on_cmd_set_test() {
        let _ = env_logger::try_init();

        let redis_test = RedisTest {};
        //let redis_test= Arc::new(redis_test);

        let mut tags: HashMap<String, String> = HashMap::with_capacity(1);
        tags.insert(Rato::DB_TAG.to_string(), "pan".to_string());
        tags.insert(Rato::AUTH_TAG.to_string(), "true".to_string());

        //let mut mulo = Rato::<RedisTest>::new(redis_test);

        let res = Rato::parse_input(&redis_test, &mut tags, &mut b"SET a b\r\n".to_vec());
        assert_eq!(String::from_utf8_lossy(&res.0), "+OK\r\n");
    }

    #[test]
    fn on_cmd_get_test() {
        let _ = env_logger::try_init();

        let redis_test = RedisTest {};
        //let redis_test= Arc::new(redis_test);

        let mut tags: HashMap<String, String> = HashMap::with_capacity(1);
        tags.insert(Rato::DB_TAG.to_string(), "pan".to_string());
        tags.insert(Rato::AUTH_TAG.to_string(), "true".to_string());

        //let mut mulo = Rato::<RedisTest>::new(redis_test);

        let res = Rato::parse_input(&redis_test, &mut tags, &mut b"GET a\r\n".to_vec());
        assert_eq!(String::from_utf8_lossy(&res.0), "$1\r\nb\r\n");
    }

    #[test]
    fn on_cmd_select_test() {
        let _ = env_logger::try_init();

        let redis_test = RedisTest {};
        //let redis_test= Arc::new(redis_test);

        let mut tags: HashMap<String, String> = HashMap::with_capacity(1);
        tags.insert(Rato::AUTH_TAG.to_string(), "true".to_string());
        //tags.insert(Rato::DB_TAG.to_string(), "pan".to_string());

        //let mut mulo = Rato::<RedisTest>::new(redis_test);

        let res = Rato::parse_input(&redis_test, &mut tags, &mut b"SELECT PAN_DB\r\n".to_vec());

        let res = Rato::parse_input(&redis_test, &mut tags, &mut b"GET a\r\n".to_vec());
        assert!(tags.contains_key(Rato::DB_TAG));
        let val = tags.get(Rato::DB_TAG);
        assert_eq!(val, Some(&"PAN_DB".to_string()));
    }
}
