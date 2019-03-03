use crate::redis_cmd::RedisCommand;
use crate::util::RedisUtil;
use hashbrown::HashMap;

pub struct Rato {}

impl Rato {
    pub const DB_TAG: &'static str = "database";
    pub const AUTH_TAG: &'static str = "auth_success";

    #[inline]
    pub fn make_pub_sub_cmd(channels: Vec<&str>) -> Vec<u8> {
        let mut output = RedisUtil::make_array(channels.len());
        for val in channels {
            output.extend(RedisUtil::make_bulk(&val.as_bytes()));
        }
        output
    }

    pub fn parse_input<T>(
        event_handler: &T,
        mut conn_tags: &mut HashMap<String, String>,
        input: &mut Vec<u8>,
    ) -> (Vec<u8>, bool)
    where
        T: ?Sized + RedisCommand,
    {
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
                let hout =
                    Rato::handle_command(event_handler, &mut conn_tags, &args, &mut close_session);

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
        (output, close)
    }

    fn handle_command<T>(
        event_handler: &T,
        conn_tags: &mut HashMap<String, String>,
        args: &[Vec<u8>],
        close_session: &mut bool,
    ) -> Vec<u8>
    where
        T: ?Sized + RedisCommand,
    {
        let db_tag = conn_tags.get(Rato::DB_TAG);

        let db = if db_tag.is_some() {
            db_tag.unwrap().as_bytes().to_vec()
        } else {
            vec![]
        };

        let mut authorize_access = false;

        let auth_status = conn_tags.get(Rato::AUTH_TAG);

        if RedisUtil::arg_match(&args[0], "AUTH")
            || (auth_status.is_some() && auth_status.unwrap() == "true")
        {
            authorize_access = true;
        }
        if !authorize_access {
            return b"-ERR You must authenticate\r\n".to_vec();
        }
        debug!(
            "DB:{}, Command:{}, Number of args:{}",
            String::from_utf8_lossy(&db),
            String::from_utf8_lossy(&args[0]),
            args.len()
        );

        if RedisUtil::arg_match(&args[0], "AUTH") {
            match args.len() {
                2 => {
                    if let Err(e) = event_handler.on_cmd_auth(&db, &args[1]) {
                        conn_tags.insert(Rato::AUTH_TAG.to_string(), "false".to_string());
                        format!("-ERR Login Failed '{}'\r\n", e.to_string())
                            .into_bytes()
                            .to_vec()
                    } else {
                        conn_tags.insert(Rato::AUTH_TAG.to_string(), "true".to_string());
                        b"+OK\r\n".to_vec()
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "SELECT") {
            match args.len() {
                2 => {
                    conn_tags.insert(
                        Rato::DB_TAG.to_string(),
                        String::from_utf8_lossy(&args[1]).to_string(),
                    );
                    b"+OK\r\n".to_vec()
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "PING") {
            match args.len() {
                1 => {
                    let v = vec![];
                    event_handler.on_cmd_ping(&v);
                    b"+PONG\r\n".to_vec()
                }
                2 => {
                    event_handler.on_cmd_ping(&args[1]);
                    RedisUtil::make_bulk(&args[1])
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "ECHO") {
            match args.len() {
                2 => {
                    event_handler.on_cmd_echo(&args[1]);
                    RedisUtil::make_bulk(&args[1])
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "SET") {
            match args.len() {
                3 => {
                    if let Err(e) = event_handler.on_cmd_set(&db, &args[1], &args[2]) {
                        format!("-ERR SET Failed '{}'\r\n", e.to_string())
                            .into_bytes()
                            .to_vec()
                    } else {
                        b"+OK\r\n".to_vec()
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "HMSET") {
            match args.len() {
                1 | 2 | 3 => RedisUtil::invalid_num_args(&args[0]),
                _ => {
                    let mut kv = HashMap::with_capacity(args.len());
                    for i in (2..args.len()).step_by(2) {
                        if i + 1 < args.len() {
                            debug!(
                                "HMSET: Key: {}, Val:{}",
                                String::from_utf8_lossy(&args[i]),
                                String::from_utf8_lossy(&args[i + 1])
                            );
                            kv.insert(&args[i], &args[i + 1]);
                        } else {
                            return RedisUtil::invalid_num_args(&args[0]);
                        }
                    }

                    if let Err(e) = event_handler.on_cmd_hmset(&db, &args[1], &kv) {
                        format!("-ERR HMSET Failed '{}'\r\n", e.to_string())
                            .into_bytes()
                            .to_vec()
                    } else {
                        b"+OK\r\n".to_vec()
                    }
                }
            }
        } else if RedisUtil::arg_match(&args[0], "HSET") {
            info!("HSET :{}", args.len());
            match args.len() {
                4 => {
                    if let Err(e) = event_handler.on_cmd_hset(&db, &args[1], &args[2], &args[3]) {
                        error!("HSET :{}", e.to_string());
                        format!("-ERR HSET Failed '{}'\r\n", e.to_string())
                            .into_bytes()
                            .to_vec()
                    } else {
                        b"+OK\r\n".to_vec()
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "FLUSHDB") {
            (if let Err(e) = event_handler.on_cmd_flushdb(&db) {
                format!("-ERR FLUSHDB Failed '{}'\r\n", e.to_string())
                    .into_bytes()
                    .to_vec()
            } else {
                b"+OK\r\n".to_vec()
            })
        } else if RedisUtil::arg_match(&args[0], "BACKUPDB") {
            (match args.len() {
                1 => {
                    if let Err(e) = event_handler.on_cmd_backupdb(&[]) {
                        format!("-ERR Backup DB Failed '{}'\r\n", e.to_string())
                            .into_bytes()
                            .to_vec()
                    } else {
                        b"+OK\r\n".to_vec()
                    }
                }
                2 => {
                    if let Err(e) = event_handler.on_cmd_backupdb(&args[1]) {
                        format!("-ERR Backup DB Failed '{}'\r\n", e.to_string())
                            .into_bytes()
                            .to_vec()
                    } else {
                        b"+OK\r\n".to_vec()
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            })
        } else if RedisUtil::arg_match(&args[0], "BACKUP_LRU_KEYS") {
            (match args.len() {
                1 => {
                    if let Err(e) = event_handler.on_cmd_backup_lru_keys(&[]) {
                        format!(
                            "-ERR BACKUP_LRU_KEYS Lru Keys Failed '{}'\r\n",
                            e.to_string()
                        )
                        .into_bytes()
                        .to_vec()
                    } else {
                        b"+OK\r\n".to_vec()
                    }
                }
                2 => {
                    if let Err(e) = event_handler.on_cmd_backup_lru_keys(&args[1]) {
                        format!(
                            "-ERR BACKUP_LRU_KEYS Lru Keys  Failed '{}'\r\n",
                            e.to_string()
                        )
                        .into_bytes()
                        .to_vec()
                    } else {
                        b"+OK\r\n".to_vec()
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            })
        } else if RedisUtil::arg_match(&args[0], "DEL") {
            match args.len() {
                2 => {
                    if let Err(_e) = event_handler.on_cmd_del(&db, &args[1]) {
                        b":0\r\n".to_vec()
                    } else {
                        b":1\r\n".to_vec()
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "GET") {
            match args.len() {
                2 => {
                    //return (RedisUtil::make_bulk(&b"abc".to_vec()), false, false);
                    match event_handler.on_cmd_get(&db, &args[1]) {
                        Ok(v) => RedisUtil::make_bulk(&v),
                        Err(_e) => {
                            // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                            b"$-1\r\n".to_vec()
                        }
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "HMGET") {
            match args.len() {
                1 | 2 => RedisUtil::invalid_num_args(&args[0]),
                _ => {
                    match event_handler.on_cmd_hmget(&db, &args[1], &args[2..]) {
                        Ok(v) => {
                            let mut output = RedisUtil::make_array(v.len());
                            for val in v {
                                //output.extend(RedisUtil::make_bulk(&key));
                                output.extend(RedisUtil::make_bulk(&val));
                            }
                            //RedisUtil::make_bulk(&output)
                            output
                        }
                        Err(_e) => {
                            // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                            b"$-1\r\n".to_vec()
                        }
                    }
                }
            }
        } else if RedisUtil::arg_match(&args[0], "HGET") {
            match args.len() {
                3 => {
                    match event_handler.on_cmd_hget(&db, &args[1], &args[2]) {
                        Ok(kv) => RedisUtil::make_bulk(&kv),
                        Err(_e) => {
                            // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                            b"$-1\r\n".to_vec()
                        }
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "HGETALL") {
            match args.len() {
                2 => {
                    match event_handler.on_cmd_hgetall(&db, &args[1]) {
                        Ok(v) => {
                            let mut output = RedisUtil::make_array(v.len() * 2);
                            for (key, val) in v {
                                output.extend(RedisUtil::make_bulk(&key));
                                output.extend(RedisUtil::make_bulk(&val));
                            }
                            // RedisUtil::make_bulk(&output)
                            output
                        }
                        Err(_e) => {
                            // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                            b"$-1\r\n".to_vec()
                        }
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "KEYS") {
            format!(
                "-ERR not supported command '{}'\r\n",
                RedisUtil::safe_line_from_slice(&args[0])
            )
            .into_bytes()
            .to_vec()
        } else if RedisUtil::arg_match(&args[0], "QUIT") {
            if let Err(_e) = event_handler.on_cmd_quit() {
                b":0\r\n".to_vec()
            } else {
                *close_session = true;
                b"+OK\r\n".to_vec()
            }
        } else if RedisUtil::arg_match(&args[0], "MESSAGE") {
            debug!("Received Command MESSAGE");
            match args.len() {
                1 | 2 => RedisUtil::invalid_num_args(&args[0]),
                _ => {
                    if let Err(_e) = event_handler.on_cmd_msg(&args[1], &args[2]) {
                        vec![]
                    } else {
                        vec![]
                    }
                }
            }
        } else if RedisUtil::arg_match(&args[0], "SUBSCRIBE") {
            match args.len() {
                3 => {
                    if let Err(_e) = event_handler.on_cmd_subscribe(&args[1]) {
                        vec![]
                    } else {
                        vec![]
                    }
                }
                4 => {
                    for arg in args.iter() {
                        debug!("Subscription response: {}", String::from_utf8_lossy(&arg));
                    }
                    if let Err(_e) = event_handler.on_cmd_subscribe_response(&args[1], &args[2]) {
                        vec![]
                    } else {
                        vec![]
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else if RedisUtil::arg_match(&args[0], "CLUSTER") {
            match args.len() {
                2 => {
                    if RedisUtil::arg_match(&args[1], "NODES") {
                        match event_handler.on_cmd_cluster_nodes() {
                            Ok(v) => RedisUtil::make_bulk(&v),
                            Err(e) => format!("-ERR CLUSTER NODES Failed '{}'\r\n", e.to_string())
                                .into_bytes()
                                .to_vec(),
                        }
                    } else if RedisUtil::arg_match(&args[1], "SLOTS") {
                        match event_handler.on_cmd_cluster_slots() {
                            Ok(v) => v, //RedisUtil::make_bulk(&v),
                            Err(e) => format!("-ERR CLUSTER SLOTS Failed '{}'\r\n", e.to_string())
                                .into_bytes()
                                .to_vec(),
                        }
                    } else {
                        RedisUtil::invalid_num_args(&args[0])
                    }
                }
                4 => {
                    if RedisUtil::arg_match(&args[1], "NODES")
                        && RedisUtil::arg_match(&args[2], "REMOVE")
                    {
                        if let Err(e) = event_handler.on_cmd_cluster_remove_node(&args[3]) {
                            format!("-ERR CLUSTER NODES REMOVE Failed '{}'\r\n", e.to_string())
                                .into_bytes()
                                .to_vec()
                        } else {
                            b"+OK\r\n".to_vec()
                        }
                    } else {
                        format!(
                            "-ERR invalid command '{}'\r\n",
                            RedisUtil::safe_line_from_slice(&args[0])
                        )
                        .into_bytes()
                        .to_vec()
                    }
                }
                13 => {
                    if !RedisUtil::arg_match(&args[1], "NODES") {
                        return format!(
                            "-ERR invalid command '{}'\r\n",
                            RedisUtil::safe_line_from_slice(&args[0])
                        )
                        .into_bytes()
                        .to_vec();
                    }
                    let mut kv = HashMap::with_capacity(args.len());
                    for i in (3..args.len()).step_by(2) {
                        if i + 1 < args.len() {
                            debug!(
                                "CLUSTER NODES ADD/UPDATE: Key: {}, Val:{}",
                                String::from_utf8_lossy(&args[i]),
                                String::from_utf8_lossy(&args[i + 1])
                            );
                            kv.insert(&args[i], &args[i + 1]);
                        } else {
                            return RedisUtil::invalid_num_args(&args[0]);
                        }
                    }
                    if RedisUtil::arg_match(&args[2], "ADD") {
                        if let Err(e) = event_handler.on_cmd_cluster_add_node(&kv) {
                            format!("-ERR CLUSTER NODES ADD Failed '{}'\r\n", e.to_string())
                                .into_bytes()
                                .to_vec()
                        } else {
                            b"+OK\r\n".to_vec()
                        }
                    } else if RedisUtil::arg_match(&args[2], "UPDATE") {
                        if let Err(e) = event_handler.on_cmd_cluster_update_node(&kv) {
                            format!("-ERR CLUSTER NODES UPDATE Failed '{}'\r\n", e.to_string())
                                .into_bytes()
                                .to_vec()
                        } else {
                            b"+OK\r\n".to_vec()
                        }
                    } else {
                        format!(
                            "-ERR invalid command '{}'\r\n",
                            RedisUtil::safe_line_from_slice(&args[0])
                        )
                        .into_bytes()
                        .to_vec()
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            }
        } else {
            format!(
                "-ERR unknown command '{}'\r\n",
                RedisUtil::safe_line_from_slice(&args[0])
            )
            .into_bytes()
            .to_vec()
        }
    }
}
