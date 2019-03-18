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
             Rato::handle_command(event_handler, conn_tags, args)

            //(vec![], false)
            });
        let res_vec: Vec < (Vec < u8 >, bool, HashMap<String, String>) > = par_iter.collect();

        res_vec
    }*/

    pub fn parse_input<T>(
        event_handler: &T,
        mut conn_tags: &mut HashMap<String, String>,
        input: &mut Vec<u8>,
        output: &mut Vec<u8>,
    ) -> bool
    where
        T: ?Sized + Sync + RedisCommand,
    {
        //let mut output = Vec::with_capacity(4096);
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

        debug!("argss len:{}, close: {}", argss.len(), close);
        let total_commands = argss.len();
        if !close && !argss.is_empty() {

            let mut authorize_access = false;

            if let Some(v) = conn_tags.get(Rato::AUTH_TAG) {
                if v == "true" {
                    authorize_access = true;
                }
            }

            ////sequential processing
            let mut close_session = false;
           if argss.len() > 10 && authorize_access {
               debug!("Using Par {}", argss.len());

               let  par_iter = argss.into_par_iter().map( |  args |{

                   Rato::handle_command(event_handler /*as &(dyn RedisCommand + Sync)*/, conn_tags, args)

               });
               let res_vec: Vec < (Vec < u8 >, bool, HashMap<String, String>) > = par_iter.collect();

               // let res_vec: Vec<(Vec<u8>, bool, _)> = Rato::process_cmd_par(argss, event_handler, &conn_tags);

               for (hout, close, _) in res_vec.iter() {
                    output.extend(hout);
                    if *close {
                        close_session = true;
                    }
                }

            }else { //sequential processing
               for args in argss.into_iter() {
                    let (hout, close_session, tags) =
                        Rato::handle_command(event_handler, &conn_tags, args);
                    *conn_tags = tags;
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
        close
    }

    fn handle_command<T>(
        event_handler: &T,
        conn_tags: &HashMap<String, String>,
        args: Vec<Vec<u8>>
    ) -> (Vec<u8>,bool, HashMap<String, String>)
    where
        T: ?Sized + RedisCommand,
    {
        let mut args = args;
        let mut close_session = false;
        let mut conn_tags = conn_tags.clone();
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
            return (b"-ERR You must authenticate\r\n".to_vec(),close_session, conn_tags);
        }
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
            },
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
            },
            b"SET" => {
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
            },
            b"HMSET" => {
                match args.len() {
                    1 | 2 | 3 => RedisUtil::invalid_num_args(&args[0]),
                    _ => {
                        if args.len() < 4 || args.len() % 2 > 0 {
                              debug!("Invalid number of arguments");
                              RedisUtil::invalid_num_args(&args[0])
                        }else {

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
            },
            b"AUTH" => {
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
            },
            b"SELECT" => {
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
            },
            b"PING" => {
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
            },
            b"ECHO" => {
                match args.len() {
                    2 => {
                        event_handler.on_cmd_echo(&args[1]);
                        RedisUtil::make_bulk(&args[1])
                    }
                    _ => RedisUtil::invalid_num_args(&args[0]),
                }
            },
            b"HSET" => {
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
            },
            b"FLUSHDB" => {
                (if let Err(e) = event_handler.on_cmd_flushdb(&db) {
                    format!("-ERR FLUSHDB Failed '{}'\r\n", e.to_string())
                        .into_bytes()
                        .to_vec()
                } else {
                    b"+OK\r\n".to_vec()
                })
            },
            b"BACKUPDB" => {
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
            },
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
            },
            b"DEL" => {
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
            },
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
            },
            b"KEYS" => {
                format!(
                    "-ERR not supported command '{}'\r\n",
                    RedisUtil::safe_line_from_slice(&args[0])
                )
                    .into_bytes()
                    .to_vec()
            },
            b"QUIT" => {
                if let Err(_e) = event_handler.on_cmd_quit() {
                    b":0\r\n".to_vec()
                } else {
                    close_session = true;
                    b"+OK\r\n".to_vec()
                }
            },
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
            },
            b"SUBSCRIBE" => {
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
            },
            b"CLUSTER" => {
                match args.len() {
                    2 => {
                        if RedisUtil::arg_match(&args[1], "NODES") {
                            match event_handler.on_cmd_cluster_nodes() {
                                Ok(v) => RedisUtil::make_bulk_extend(v),
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
                            return ( format!(
                                "-ERR invalid command '{}'\r\n",
                                RedisUtil::safe_line_from_slice(&args[0])
                            ).into_bytes().to_vec(), close_session, conn_tags)
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
                                return (RedisUtil::invalid_num_args(&args[0]), close_session, conn_tags)
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
            },
            _ => {
                format!(
                    "-ERR unknown command '{}'\r\n",
                    RedisUtil::safe_line_from_slice(&args[0])
                )
                    .into_bytes()
                    .to_vec()
            }
        };

        (output_buffer, close_session, conn_tags)
    }

}
