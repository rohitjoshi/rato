use crate::redis_cmd::RedisCommand;
use crate::util::RedisUtil;
use hashbrown::HashMap;
use rayon::prelude::*;

pub struct Rato {}

//unsafe impl Send for Rato {}
//unsafe impl Sync for Rato {}

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

    /*fn process_cmd_par<T>(argss: Vec<Vec<Vec<u8>>>,  event_handler: &T,
                             conn_tags: &HashMap<String, String>) -> Vec<(Vec<u8>,bool, HashMap<String, String>)> where
        T: Sync + RedisCommand {

        let  par_iter = argss.into_par_iter().map( |  args |{
           // let conn_tags :HashMap<String, String>= HashMap::new();
            //let args = args.clone();
             Rato::handle_command(event_handler, args)

            //(vec![], false)
            });
        let res_vec: Vec < (Vec < u8 >, bool, HashMap<String, String>) > = par_iter.collect();

        res_vec
    }*/

    ///
    /// is authorized access
    ///
    fn is_authorized_access<T>(
        argss: &mut Vec<Vec<Vec<u8>>>,
        event_handler: &T,
        conn_tags: &mut HashMap<String, String>,
        output: &mut Vec<u8>,
    ) -> bool
    where
        T: ?Sized + Sync + RedisCommand,
    {
        debug!("is_authorized_access");
        let mut authorize_access = false;
        if let Some(auth_status) = conn_tags.get(Rato::AUTH_TAG) {
            if auth_status == "true" {
                debug!("Authorized access");
                return true;
            }
        }
        if !argss[0].is_empty() && argss[0][0].as_slice() == b"AUTH" {
            let mut db = vec![];
            if let Some(db_tag) = conn_tags.get(Rato::DB_TAG) {
                db = db_tag.as_bytes().to_vec();
            };
            let args = &argss[0];
            match args.len() {
                2 => {
                    debug!("Command AUTH received with password");
                    if let Err(e) = event_handler.on_cmd_auth(&db, &args[1]) {
                        conn_tags.insert(Rato::AUTH_TAG.to_string(), "false".to_string());
                        output.extend(
                            format!("-ERR Login Failed '{}'\r\n", e.to_string())
                                .into_bytes()
                                .to_vec(),
                        );
                    } else {
                        conn_tags.insert(Rato::AUTH_TAG.to_string(), "true".to_string());
                        authorize_access = true;
                        output.extend(b"+OK\r\n".to_vec());
                    }
                    argss.drain(0..1);
                }
                _ => {
                    warn!("Unsupported command: {}", String::from_utf8_lossy(&args[0]));
                    output.extend(RedisUtil::invalid_num_args(&args[0]));
                    argss.drain(0..1);
                }
            }
        } else if !argss[0].is_empty() && !argss[0][0].is_empty() {
            output.extend(
                format!(
                    "-ERR Command received {}. Must send AUTH command first\r\n",
                    String::from_utf8_lossy(&argss[0][0])
                )
                .into_bytes()
                .to_vec(),
            );
            argss.drain(0..1);
        } else {
            output.extend(
                format!("-ERR Must send AUTH command first\r\n")
                    .into_bytes()
                    .to_vec(),
            );
        }
        authorize_access
    }

    fn process_db_tag(
        argss: &mut Vec<Vec<Vec<u8>>>,
        conn_tags: &mut HashMap<String, String>,
        output: &mut Vec<u8>,
    ) -> (Vec<u8>, bool) {
        if !argss.is_empty() && !argss[0].is_empty() && argss[0][0].as_slice() == b"SELECT" {
            let args = &argss[0];
            match args.len() {
                2 => {
                    conn_tags.insert(
                        Rato::DB_TAG.to_string(),
                        String::from_utf8_lossy(&args[1]).to_string(),
                    );
                    output.extend(b"+OK\r\n".to_vec());
                    let db = args[1].to_vec();
                    argss.drain(0..1);
                    return (db, true);
                }
                _ => {
                    output.extend(RedisUtil::invalid_num_args(&args[0]));
                    argss.drain(0..1);
                    return (vec![], false);
                }
            }
        }
        (vec![], false)
    }

    pub fn parse_input<T>(
        event_handler: &T,
        conn_tags: &mut HashMap<String, String>,
        input: &mut Vec<u8>,
        output: &mut Vec<u8>,
        parallel_parsing_threshold: usize,
    ) -> (bool, usize)
    where
        T: ?Sized + Sync + RedisCommand,
    {
        let mut close = false;
        let mut i = 0;
        let mut argss = Vec::with_capacity(100);
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

        let total_commands = argss.len();
        debug!(
            "rato: number of commands:{}, is_close: {}, input_len:{}, remaining: {}",
            total_commands,
            close,
            input.len(),
            input.len() - i
        );

        let mut db = vec![];

        if let Some(db_tag) = conn_tags.get(Rato::DB_TAG) {
            db = db_tag.as_bytes().to_vec();
        }
        // if args not empty and if fields are either SELECT or AUTH process here
        if !argss.is_empty() {
            if !Rato::is_authorized_access(&mut argss, event_handler, conn_tags, output) {
                warn!("Unauthorized access");
                debug!(
                    "Redis Command response: {}",
                    String::from_utf8_lossy(&output)
                );
                input.clear();
                return (false, total_commands); // close
            }
            if !argss.is_empty() {
                let (db_name, found_db) = Rato::process_db_tag(&mut argss, conn_tags, output);
                if found_db {
                    db = db_name;
                }
            }
        }

        //in processing db and auth, we would have drained
        if !close && !argss.is_empty() {
            ////sequential processing

            if argss.len() >= parallel_parsing_threshold {
                debug!("Using Par {}", argss.len());

                let par_iter = argss
                    .into_par_iter()
                    .map(|args| Rato::handle_command(event_handler, &db, args));
                let res_vec: Vec<(Vec<u8>, bool)> = par_iter.collect();
                for (hout, close_session) in res_vec.iter() {
                    output.extend(hout);
                    if *close_session {
                        close = true;
                    }
                }
            } else {
                //sequential processing
                for args in argss.into_iter() {
                    let (hout, close_session) = Rato::handle_command(event_handler, &db, args);
                    output.extend(hout);
                    if close_session {
                        close = true;
                        break;
                    }
                }
            }
        }
        if i > 0 {
            if i < input.len() {
                //let mut remain = Vec::with_capacity(input.len() - i);
                //remain.extend_from_slice(&input[i..input.len()]);
                //debug!("Remaning buffer:{}", String::from_utf8_lossy(&remain));
                //input.clear();
                input.drain(0..i);
            //input.extend(remain)
            } else {
                input.clear()
            }
        }
        // (output, close)
        debug!(
            "Redis Command response: {}",
            String::from_utf8_lossy(&output)
        );
        (close, total_commands)
    }

    fn handle_command<T>(event_handler: &T, db: &[u8], args: Vec<Vec<u8>>) -> (Vec<u8>, bool)
    where
        T: ?Sized + RedisCommand,
    {
        //let mut args = args;
        let mut close_session = false;

        debug!(
            "handle_command():DB:{}, Command:{}, Number of args:{}",
            String::from_utf8_lossy(&db),
            String::from_utf8_lossy(&args[0]),
            args.len()
        );

        let output_buffer = match args[0].as_slice() {
            b"GET" => {
                match args.len() {
                    2 => {
                        //return (RedisUtil::make_bulk(&b"abc".to_vec()), false, false);
                        match event_handler.on_cmd_get(&db, &args[1]) {
                            Ok(Some(v)) => RedisUtil::make_bulk_extend(v),

                            _ => {
                                // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                                b"$-1\r\n".to_vec()
                            }
                        }
                    }
                    _ => RedisUtil::invalid_num_args(&args[0]),
                }
            }
            b"HMGET" => {
                match args.len() {
                    1 | 2 => RedisUtil::invalid_num_args(&args[0]),
                    _ => {
                        match event_handler.on_cmd_hmget(&db, &args[1], &args[2..]) {
                            Ok(Some(resp)) => {
                                let mut output = RedisUtil::make_array(resp.len());
                                for val in resp.into_iter() {
                                    //output.extend(RedisUtil::make_bulk(&key));
                                    if val.is_empty() {
                                        output.extend(b"$-1\r\n");
                                    } else {
                                        output.extend(RedisUtil::make_bulk_extend(val));
                                    }
                                }
                                output
                            }
                            _ => {
                                // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                                b"$-1\r\n".to_vec()
                            }
                        }
                    }
                }
            }
            b"SET" => match args.len() {
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
            },
            b"HMSET" => {
                match args.len() {
                    1 | 2 | 3 => RedisUtil::invalid_num_args(&args[0]),
                    _ => {
                        if args.len() < 4 || args.len() % 2 > 0 {
                            debug!("Invalid number of arguments");
                            RedisUtil::invalid_num_args(&args[0])
                        } else {
                            //TODO:  Make last parameter pass by move

                            if let Err(e) = event_handler.on_cmd_hmset(&db, &args[1], &args[2..]) {
                                format!("-ERR HMSET Failed '{}'\r\n", e.to_string())
                                    .into_bytes()
                                    .to_vec()
                            } else {
                                b"+OK\r\n".to_vec()
                            }
                        }
                    }
                }
            }
            b"AUTH" => {
                b"-ERR AUTH Failed 'Not supported in pipeline mode'\r\n".to_vec()

                /* match args.len() {
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
                }*/
            }
            b"SELECT" => {
                b"-ERR SELECT Failed 'Not supported in pipeline mode'\r\n".to_vec()
                /*
                match args.len() {
                    2 => {
                        conn_tags.insert(
                            Rato::DB_TAG.to_string(),
                            String::from_utf8_lossy(&args[1]).to_string(),
                        );
                        b"+OK\r\n".to_vec()
                    }
                    _ => RedisUtil::invalid_num_args(&args[0]),
                }*/
            }
            b"PING" => match args.len() {
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
            },
            b"ECHO" => match args.len() {
                2 => {
                    event_handler.on_cmd_echo(&args[1]);
                    RedisUtil::make_bulk(&args[1])
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            },
            b"HSET" => {
                info!("HSET :{}", args.len());
                match args.len() {
                    4 => {
                        if let Err(e) = event_handler.on_cmd_hset(&db, &args[1], &args[2], &args[3])
                        {
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
            }
            b"FLUSHDB" => {
                (if let Err(e) = event_handler.on_cmd_flushdb(&db) {
                    format!("-ERR FLUSHDB Failed '{}'\r\n", e.to_string())
                        .into_bytes()
                        .to_vec()
                } else {
                    b"+OK\r\n".to_vec()
                })
            }
            b"BACKUPDB" => {
                (match args.len() {
                    1 => {
                        if let Err(e) = event_handler.on_cmd_backupdb(&[]) {
                            format!("-ERR Backup DB Failed '{}'\r\n", e.to_string())
                                .into_bytes()
                                .to_vec()
                        } else {
                            b"+OK Backup started\r\n".to_vec()
                        }
                    }
                    2 => {
                        if let Err(e) = event_handler.on_cmd_backupdb(&args[1]) {
                            format!("-ERR Backup DB Failed '{}'\r\n", e.to_string())
                                .into_bytes()
                                .to_vec()
                        } else {
                            b"+OK Backup started\r\n".to_vec()
                        }
                    }
                    _ => RedisUtil::invalid_num_args(&args[0]),
                })
            }
            b"BACKUP_LRU_KEYS" => {
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
                            b"+OK Backup started\r\n".to_vec()
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
                            b"+OK Backup started\r\n".to_vec()
                        }
                    }
                    _ => RedisUtil::invalid_num_args(&args[0]),
                })
            }
            b"DEL" => match args.len() {
                2 => {
                    if let Err(_e) = event_handler.on_cmd_del(&db, &args[1]) {
                        b":0\r\n".to_vec()
                    } else {
                        b":1\r\n".to_vec()
                    }
                }
                _ => RedisUtil::invalid_num_args(&args[0]),
            },
            b"HGET" => {
                match args.len() {
                    3 => {
                        match event_handler.on_cmd_hget(&db, &args[1], &args[2]) {
                            Ok(Some(kv)) => RedisUtil::make_bulk_extend(kv),
                            _ => {
                                // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                                b"$-1\r\n".to_vec()
                            }
                        }
                    }
                    _ => RedisUtil::invalid_num_args(&args[0]),
                }
            }
            b"HGETALL" => {
                match args.len() {
                    2 => {
                        match event_handler.on_cmd_hgetall(&db, &args[1]) {
                            Ok(Some(v)) => {
                                let mut output = RedisUtil::make_array(v.len());
                                for key_val in v.into_iter() {
                                    output.extend(RedisUtil::make_bulk_extend(key_val));
                                }
                                // RedisUtil::make_bulk(&output)
                                output
                            }
                            _ => {
                                // error!("GET: Received error while getting from store for key: {}. Error:{}",String::from_utf8_lossy(&args[1]), e.to_string());
                                b"$-1\r\n".to_vec()
                            }
                        }
                    }
                    _ => RedisUtil::invalid_num_args(&args[0]),
                }
            }
            b"KEYS" => format!(
                "-ERR not supported command '{}'\r\n",
                RedisUtil::safe_line_from_slice(&args[0])
            )
            .into_bytes()
            .to_vec(),
            b"QUIT" => {
                if let Err(_e) = event_handler.on_cmd_quit() {
                    b":0\r\n".to_vec()
                } else {
                    close_session = true;
                    b"+OK\r\n".to_vec()
                }
            }
            b"MESSAGE" => {
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
            }
            b"SUBSCRIBE" => match args.len() {
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
            },
            b"CLUSTER" => {
                match args.len() {
                    2 => {
                        if RedisUtil::arg_match(&args[1], "NODES") {
                            match event_handler.on_cmd_cluster_nodes() {
                                Ok(v) => RedisUtil::make_bulk_extend(v),
                                Err(e) => {
                                    format!("-ERR CLUSTER NODES Failed '{}'\r\n", e.to_string())
                                        .into_bytes()
                                        .to_vec()
                                }
                            }
                        } else if RedisUtil::arg_match(&args[1], "SLOTS") {
                            match event_handler.on_cmd_cluster_slots() {
                                Ok(v) => v, //RedisUtil::make_bulk(&v),
                                Err(e) => {
                                    format!("-ERR CLUSTER SLOTS Failed '{}'\r\n", e.to_string())
                                        .into_bytes()
                                        .to_vec()
                                }
                            }
                        } else {
                            format!(
                                "-ERR not supported command CLUSTER '{}'\r\n",
                                RedisUtil::safe_line_from_slice(&args[1])
                            )
                            .into_bytes()
                            .to_vec()
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
                            return (
                                format!(
                                    "-ERR invalid command '{}'\r\n",
                                    RedisUtil::safe_line_from_slice(&args[0])
                                )
                                .into_bytes()
                                .to_vec(),
                                close_session,
                            );
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
                                return (RedisUtil::invalid_num_args(&args[0]), close_session);
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
            }
            _ => format!(
                "-ERR unknown command '{}'\r\n",
                RedisUtil::safe_line_from_slice(&args[0])
            )
            .into_bytes()
            .to_vec(),
        };

        (output_buffer, close_session)
    }
}
