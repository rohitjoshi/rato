#[macro_use]
extern crate log;

pub mod util;
pub mod redis_cmd;
use std::collections::HashMap;
use self::util::RedisUtil;
use self::redis_cmd::RedisCommand;


struct Rato<T> {

    event_handler: T,

}

impl<T> Rato<T> where T: RedisCommand {

    pub fn new(ev_handler: T) -> Rato<T>{
        Rato {
            event_handler:ev_handler
        }
    }

    pub fn parse_input(&self,  input: &mut Vec<u8>)->Vec<u8> {
        let mut output = Vec::new();
        let mut close = false;
        let mut i = 0;
        let mut argss = Vec::new();
        debug!(
            "Redis Command/Payload received: {}",
            String::from_utf8_lossy(&input)
        );

        loop {
            let (args, err, ni, complete) = RedisUtil::redcon_take_args(input, i);
            if err != "" {
                output.extend(format!("-{}\r\n", err).into_bytes());
                close = true;
                error!(
                    "Failed to parse redis response. Error:{}",
                    String::from_utf8_lossy(&output)
                );
                break;
            } else if !complete {
                break;
            }
            i = ni;
            if !args.is_empty() {
                argss.push(args);
            }
        }

        debug!("argss len:{}, close: {}", argss.len(), close);

        if !close && !argss.is_empty() {
            //let mut aof = Vec::new();
            //let mut store = store.lock().unwrap();
            for args in argss {
                let mut close_session = false;
                //let (hout, write, hclose) = handle_command_dummy(&args);
                let hout = self.handle_command(&args, close_session);


                output.extend_from_slice(hout.as_slice());
                if close_session {
                    close = true;
                    break;
                }
                
            }
            // if aof.len() > 0 {
            //     // FUTURE: persist to disk
            // }
        }
         if i > 0 {
            if i < input.len() {
                let mut remain = Vec::with_capacity(input.len() - i);
                remain.extend_from_slice(&input[i..input.len()]);
                debug!("Remaning buffer:{}", String::from_utf8_lossy(&remain));
                input.clear();
                input.extend(remain)
            } else {
                input.clear()
            }
        }
       // (output, close)
        debug!(
            "Redis Command response: {}",
            String::from_utf8_lossy(&output)
        );
        output
    }


    pub fn handle_command(
        &self,
        args: &Vec<Vec<u8>>,
        mut close_session: bool

    ) -> Vec<u8> {

        debug!("Command:{}, Number of args:{}", String::from_utf8_lossy(&args[0]),args.len());

        if RedisUtil::arg_match(&args[0], "AUTH") {
            match args.len() {
                2 => {
                    if let Err(e) = self.event_handler.on_cmd_auth(&args[1]) {
                        format!(
                            "-ERR Login Failed '{}'\r\n",
                            e.to_string()
                        ).into_bytes()
                            .to_vec()
                    }else {
                        b"+OK\r\n".to_vec()
                    }
                },
                _ => RedisUtil::invalid_num_args(&args[0])
            }
        } else if RedisUtil::arg_match(&args[0], "PING") {
           match args.len() {
                1 => {
                    let v = vec![];
                    self.event_handler.on_cmd_ping(&v);
                    b"+PONG\r\n".to_vec()
                },
                2 => {
                    self.event_handler.on_cmd_ping(&args[1]);
                    RedisUtil::make_bulk(&args[1])
                },
                _ => RedisUtil::invalid_num_args(&args[0])
            }
        } else if RedisUtil::arg_match(&args[0], "ECHO") {
            match args.len() {
                2 => {
                    self.event_handler.on_cmd_echo(&args[1]);
                    RedisUtil::make_bulk(&args[1])
                },
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "SET") {
            match args.len() {
                3 => {
                    if let Err(e) =   self.event_handler.on_cmd_set(&args[1], &args[2]) {

                            format!(
                                "-ERR SET Failed '{}'\r\n",
                                e.to_string()
                            ).into_bytes()
                                  .to_vec()
                        }else {
                        b"+OK\r\n".to_vec()
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "HMSET")
            {
                match args.len() {
                    1 | 2 | 3 => RedisUtil::invalid_num_args(&args[0]),
                    _ => {
                        let mut kv  = HashMap::with_capacity(args.len());
                        for i in (2..args.len()).step_by(2) {
                            if i+1 < args.len() {
                                kv.insert(&args[i], &args[i + 1]);
                            }else {
                                return RedisUtil::invalid_num_args(&args[0]);
                            }
                        }

                        if let Err(e) =   self.event_handler.on_cmd_hmset(&args[1], &kv) {

                            format!(
                                "-ERR HMSET Failed '{}'\r\n",
                                e.to_string()
                            ).into_bytes()
                                  .to_vec()
                        }else {
                             b"+OK\r\n".to_vec()
                        }
                    }
                }
            }else if  RedisUtil::arg_match(&args[0], "HSET")
            {
                match args.len() {
                    4 => {
                        if let Err(e) =   self.event_handler.on_cmd_hset(&args[1], &args[2], &args[3]) {

                            format!(
                                "-ERR HSET Failed '{}'\r\n",
                                e.to_string()
                            ).into_bytes()
                                  .to_vec()
                        }else {
                            b"+OK\r\n".to_vec()
                        }
                    },
                    _ => RedisUtil::invalid_num_args(&args[0]),
                }
            } else if RedisUtil::arg_match(&args[0], "FLUSHDB") {
            (
                if let Err(e) =   self.event_handler.on_cmd_flushdb() {
                    format!(
                        "-ERR FLUSHDB Failed '{}'\r\n",
                        e.to_string()
                    ).into_bytes()
                          .to_vec()
                }else {
                    b"+OK\r\n".to_vec()
                }
            )
        } else if RedisUtil::arg_match(&args[0], "DEL") {
            match args.len() {
                2 => {
                    if let Err(e) =   self.event_handler.on_cmd_del(&args[1]) {
                        b":0\r\n".to_vec()
                    }else {
                        b":1\r\n".to_vec()
                    }

                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "GET") {
            match args.len() {
                2 => {
                    //return (RedisUtil::make_bulk(&b"abc".to_vec()), false, false);
                    match  self.event_handler.on_cmd_get(&args[1]) {
                        Ok(v) => RedisUtil::make_bulk(&v),
                        Err(_e) => {
                            // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                            b"$-1\r\n".to_vec()
                        }
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0])
            }
        } else if RedisUtil::arg_match(&args[0], "HMGET") || RedisUtil::arg_match(&args[0], "HGET")
            || RedisUtil::arg_match(&args[0], "HGETALL")
            {
                match args.len() {
                    1 => RedisUtil::invalid_num_args(&args[0]),
                    2 => {
                        match  self.event_handler.on_cmd_hgetall(&args[1]) {
                            Ok(v) =>
                                {
                                    let mut output = RedisUtil::make_array(v.len());
                                    for (key, val) in v {
                                        output.extend(RedisUtil::make_bulk(&key.as_bytes().to_vec()));
                                        output.extend(RedisUtil::make_bulk(&val.as_bytes().to_vec()));
                                    }
                                    RedisUtil::make_bulk(&output)
                                }
                            Err(_e) => {
                                // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                                b"$-1\r\n".to_vec()
                            }
                        }
                    }
                    3 => {
                        match  self.event_handler.on_cmd_hget(&args[1], &args[2]) {
                            Ok(kv) =>{
                                RedisUtil::make_bulk(&kv)
                            },
                            Err(_e) => {
                                // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                                b"$-1\r\n".to_vec()
                            }
                        }
                    }
                    _ => {
                        format!(
                            "-ERR not supported command '{}'\r\n",
                            RedisUtil::safe_line_from_slice(&args[0])
                        ).into_bytes()
                            .to_vec()}
                }
            } else if RedisUtil::arg_match(&args[0], "KEYS") {

                format!(
                    "-ERR not supported command '{}'\r\n",
                    RedisUtil::safe_line_from_slice(&args[0])
                ).into_bytes()
                    .to_vec()

        } else if RedisUtil::arg_match(&args[0], "QUIT") {
            if let Err(e) =   self.event_handler.on_cmd_quit() {
                b":0\r\n".to_vec()
            }else {
                close_session = true;
                b"+OK\r\n".to_vec()
            }
        } else if RedisUtil::arg_match(&args[0], "MESSAGE") {
            debug!("Received Command MESSAGE");
            match args.len() {
                1 | 2 => RedisUtil::invalid_num_args(&args[0]),
                _ => {
                    if let Err(e) =   self.event_handler.on_cmd_msg(&args[1]) {
                        vec![]
                    }else {
                        vec![]
                    }

                }
            }
        } else if RedisUtil::arg_match(&args[0], "SUBSCRIBE") {
            match args.len() {
                _ => {
                    for arg in args.iter() {
                        debug!("Subscription response: {}", String::from_utf8_lossy(&arg));
                    }
                    if let Err(e) =   self.event_handler.on_cmd_subscribe(&args[1]) {
                        vec![]
                    }else {
                        vec![]
                    }
                }
            }
        } else if RedisUtil::arg_match(&args[0], "CLUSTER") {
            match args.len() {
                2 => {
                    if RedisUtil::arg_match(&args[1], "NODES") {
                       match  self.event_handler.on_cmd_cluster_nodes() {
                            Ok(v) => RedisUtil::make_bulk(&v),
                            Err(e) => {
                                format!(
                                    "-ERR CLUSTER NODES Failed '{}'\r\n",
                                    e.to_string()
                                ).into_bytes()
                                    .to_vec()
                            }
                        }

                    } else if RedisUtil::arg_match(&args[1], "SLOTS") {
                        match  self.event_handler.on_cmd_cluster_slots() {
                            Ok(v) => RedisUtil::make_bulk(&v),
                            Err(e) => {
                                format!(
                                    "-ERR CLUSTER SLOTS Failed '{}'\r\n",
                                    e.to_string()
                                ).into_bytes()
                                    .to_vec()
                            }
                        }


                    } else {
                        RedisUtil::invalid_num_args(&args[0])
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0])
            }
        } else {

                format!(
                    "-ERR unknown command '{}'\r\n",
                    RedisUtil::safe_line_from_slice(&args[0])
                ).into_bytes()
                    .to_vec()

        }
    }


}


#[cfg(test)]
mod tests {


   use super::*;
    extern crate env_logger;


    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn on_cmd_ping_test() {
        let _ = env_logger::try_init();
        struct RedisTest {};

        impl RedisCommand for RedisTest {
            fn on_cmd_ping(&self,key: &[u8]) {
                if !key.is_empty() {
                    let test_key = b"test_key";
                    assert_eq!(key,test_key);
                }
            }
            fn on_cmd_echo(&self,val: &[u8]) {}
            fn on_cmd_quit(&self)->Result<(),String> {
                Err("Not implemented".to_string())
            }

            fn on_cmd_auth(&self,password: &[u8])->Result<(), String> {
                if !password.is_empty() && password == b"secret"{
                    return Ok(());
                }
                Err("Invalid password".to_string())
            }
            fn on_cmd_flushdb(&self)->Result<(),String> {
                Err("Not implemented".to_string())
            }
            fn on_cmd_keys(&self)->Result<Vec<Vec<u8>>,String>{
                Err("Not implemented".to_string())
            }


            fn on_cmd_del(&self,key: &[u8])->Result<(),String>{
                Err("Not implemented".to_string())
            }
            fn on_cmd_get(&self,key: &[u8])->Result<Vec<u8>,String>{
                Ok(b"b".to_vec())
            }
            fn on_cmd_hmget(&self,hash: &[u8], keys: &[Vec<u8>])->Result<Vec<Vec<u8>>,String>{
                Err("Not implemented".to_string())
            }
            fn on_cmd_hget(&self,hash: &[u8], key: &[u8])->Result<Vec<u8>,String>{
                Err("Not implemented".to_string())
            }
            fn on_cmd_hgetall(&self,hash: &[u8])->Result<HashMap<String, String>,String>{
                Err("Not implemented".to_string())
            }
            fn on_cmd_set(&self,key: &[u8], val: &[u8])->Result<(),String>{
               Ok(())
            }
            fn on_cmd_hmset(&self,hash: &[u8], kv: &HashMap<&Vec<u8>, &Vec<u8>>)->Result<(),String>{
                Err("Not implemented".to_string())
            }
            fn on_cmd_hset(&self,hash: &[u8],  key: &[u8], val: &[u8])->Result<(),String>{
                Err("Not implemented".to_string())
            }

            fn on_cmd_msg(&self,val: &[u8])->Result<(),String>{
                Err("Not implemented".to_string())
            }
            fn on_cmd_subscribe(&self,val: &[u8])->Result<(),String>{
                Err("Not implemented".to_string())
            }
            fn on_cmd_cluster_nodes(&self)->Result<Vec<u8>,String>{
                Err("Not implemented".to_string())
            }
            fn on_cmd_cluster_slots(&self)->Result<Vec<u8>,String>{
                Err("Not implemented".to_string())
            }
        }

        let redis_test = RedisTest {};

        let mut mulo = Rato::<RedisTest>::new(redis_test);

        let res = mulo.parse_input(&mut b"AUTH secret\r\n".to_vec());
        assert_eq!(String::from_utf8_lossy(&res), "+OK\r\n");

        let res = mulo.parse_input(&mut b"AUTH abc\r\n".to_vec());
        assert_ne!(String::from_utf8_lossy(&res), "+OK\r\n");

        let res = mulo.parse_input(&mut b"PING\r\n".to_vec());
        assert_eq!(String::from_utf8_lossy(&res), "+PONG\r\n");

        let res = mulo.parse_input(&mut b"PING test_key\r\n".to_vec());
        assert_eq!(String::from_utf8_lossy(&res), "$8\r\ntest_key\r\n");

        let res = mulo.parse_input(&mut b"SET a b\r\n".to_vec());
        assert_eq!(String::from_utf8_lossy(&res), "+OK\r\n");

        let res = mulo.parse_input(&mut b"GET a\r\n".to_vec());
        assert_eq!(String::from_utf8_lossy(&res), "$1\r\nb\r\n");





    }
}
