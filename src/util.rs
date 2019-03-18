///
/// Some of the Code copied from: https://github.com/tidwall/cache-server
/// Copy right for copied code: Josh Baker @tidwall,  MIT License
///

pub struct RedisUtil {}

impl RedisUtil {
    #[inline]
    pub fn redcon_take_args(input: &[u8], ni: usize) -> (Vec<Vec<u8>>, String, usize, bool) {
        if input.len() > ni {
            if input[ni] == b'*' {
                let (mut args, err, index, complete) = RedisUtil::redcon_take_multibulk_args(input, ni);
                if !args.is_empty() {
                    args[0].make_ascii_uppercase();
                }
                (args, err, index, complete)

            } else {
                let (mut args, err, index, complete) = RedisUtil::redcon_take_inline_args(input, ni);
                if !args.is_empty() {
                    args[0].make_ascii_uppercase();
                }
                (args, err, index, complete)
            }
        } else {
            (Vec::default(), String::default(), ni, false)
        }
    }

    #[inline]
    pub fn safe_line_from_string(s: &str) -> String {
        RedisUtil::safe_line_from_slice(s.as_bytes())
    }

    #[inline]
    pub fn safe_line_from_slice(s: &[u8]) -> String {
        let mut out = Vec::new();
        for c in s {
            if *c < b' ' {
                out.push(b' ')
            } else {
                out.push(*c);
            }
        }
        String::from_utf8_lossy(out.as_slice()).to_string()
    }

    pub fn arg_match(arg: &[u8], what: &str) -> bool {
        if arg.len() != what.len() {
            return false;
        }
        let what = what.as_bytes();
        for i in 0..arg.len() {
            if arg[i] != what[i] {
                if arg[i] >= b'a' && arg[i] <= b'z' {
                    if arg[i] != what[i] + 32 {
                        return false;
                    }
                } else if arg[i] >= b'A' && arg[i] <= b'Z' && arg[i] != what[i] - 32 {
                    return false;
                }
            }
        }
        true
    }

    #[inline]
    pub fn make_bulk(bulk: &[u8]) -> Vec<u8> {
        let mut resp = Vec::with_capacity(bulk.len() + 10);
        resp.push(b'$');
        resp.extend_from_slice(&bulk.len().to_string().into_bytes());
        resp.push(b'\r');
        resp.push(b'\n');
        resp.extend(bulk);
        resp.push(b'\r');
        resp.push(b'\n');
        resp
    }

    #[inline]
    pub fn make_bulk_extend(bulk: Vec<u8>) -> Vec<u8> {
        let mut resp = Vec::with_capacity(bulk.len() + 10);
        resp.push(b'$');
        resp.extend_from_slice(&bulk.len().to_string().into_bytes());
        resp.push(b'\r');
        resp.push(b'\n');
        resp.extend(bulk);
        resp.push(b'\r');
        resp.push(b'\n');
        resp
    }

    pub fn make_array(count: usize) -> Vec<u8> {
        let mut resp = Vec::new();
        resp.push(b'*');
        if count > 0 {
            resp.extend_from_slice(&count.to_string().into_bytes());
        } else {
            resp.extend_from_slice(b"1\r\n$-1");
        }
        resp.push(b'\r');
        resp.push(b'\n');
        resp
    }

    #[inline]
    pub fn invalid_num_args(cmd: &[u8]) -> Vec<u8> {
        format!(
            "-ERR wrong number of arguments for '{}' command\r\n",
            String::from_utf8_lossy(&cmd)
        )
        .into_bytes()
        .to_vec()
    }

    pub fn redcon_take_multibulk_args(
        input: &[u8],
        ni: usize,
    ) -> (Vec<Vec<u8>>, String, usize, bool) {
        trace!("redcon_take_multibulk_args()");
        let mut err = String::default();
        let mut complete = false;
        let mut args: Vec<Vec<u8>> = Vec::new();
        let mut i = ni + 1;
        let mut s = ni;
        while i < input.len() {
            if input[i - 1] == b'\r' && input[i] == b'\n' {
                match String::from_utf8_lossy(&input[s + 1..i - 1]).parse::<usize>() {
                    Ok(nargs) => {
                        trace!("nargs:{}", nargs);
                        i += 1;
                        complete = nargs == 0;
                        for ag in 0..nargs {
                            trace!("ag:{}", ag);
                            s = i;
                            while i < input.len() {
                                if input[i - 1] == b'\r' && input[i] == b'\n' {
                                    //let mut next_index = s;

                                    // this is response status. e.g for subscriber command :0 or :1
                                    let mut response_flag = false;
                                    if input[s] == b':' {
                                        response_flag = true;
                                    } else if input[s] != b'$' {
                                        err = format!("expected '$', got '{}'", input[s] as char);
                                        error!(
                                            "Redis parse error: {},  s:{}, i:{}, ni:{}, nargs:{}, input_len:{}, Payload:{}",
                                            err,
                                            s,
                                            i,
                                            ni,
                                            nargs,
                                            input.len(),
                                            String::from_utf8_lossy(&input[s - 15..])
                                        );

                                        break;
                                    }
                                    //next_index += 1;
                                    match String::from_utf8_lossy(&input[s + 1..i - 1])
                                        .parse::<usize>()
                                    {
                                        Ok(nbytes) => {
                                            trace!("nbytes:{}", nbytes,);
                                            if response_flag {
                                                trace!("response_flag is true");
                                                let bin = input[s + 1..i - 1].to_vec();
                                                let response_status_len = bin.len();
                                                trace!(
                                                    "Adding to args: {}",
                                                    String::from_utf8_lossy(&bin)
                                                );
                                                i = i + 1 + bin.len() + 2;
                                                args.push(bin);
                                                trace!("args len: {}", response_status_len);
                                            } else {
                                                if input.len() < i + 1 + nbytes + 2 {
                                                    trace!(
                                                            "Break in input.len() < i + 1 + nbytes + 2. Remaning payload:{}",
                                                            String::from_utf8_lossy(&input[s + 1..])
                                                        );
                                                    return (args, err, i, false);
                                                    //break;
                                                }
                                                let bin = input[i + 1..i + 1 + nbytes].to_vec();
                                                trace!(
                                                    "Adding to args: {}",
                                                    String::from_utf8_lossy(&bin)
                                                );
                                                args.push(bin);
                                                trace!("args len: {}", args.len());
                                                i = i + 1 + nbytes + 2;
                                            }
                                        }
                                        Err(_) => {
                                            err = "invalid bulk length".to_string();
                                            error!("Redis parse error: {}", err);
                                        }
                                    }
                                    break;
                                }
                                i += 1;
                            }
                            if err != "" {
                                error!("Redis parse error: {}. Breaking loop", err);
                                break;
                            }
                            if args.len() == nargs {
                                trace!(
                                    "Complete true: args.len(): {} == nargs: {}",
                                    args.len(),
                                    nargs
                                );
                                complete = true;
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        err = "invalid multibulk length".to_string();
                        error!("Redis parse error: {}", err);
                    }
                }
                break;
            }
            i += 1;
        }
        if err != "" {
            error!("Redis ERR Protocol error: {}", err);

            err = format!(
                "ERR Protocol error: {}",
                RedisUtil::safe_line_from_string(&err)
            )
        }
        trace!(
            "Returning args leng:{}, err:{}, i:{}, complete:{}",
            args.len(),
            err,
            i,
            complete
        );
        (args, err, i, complete)
    }

    pub fn redcon_take_inline_args(
        packet: &[u8],
        ni: usize,
    ) -> (Vec<Vec<u8>>, String, usize, bool) {
        let mut i = ni;
        let mut s = ni;
        let mut args: Vec<Vec<u8>> = Vec::new();
        while i < packet.len() {
            if packet[i] == b' ' || packet[i] == b'\n' {
                let ii = if packet[i] == b'\n' && i > s && packet[i - 1] == b'\r' {
                    i - 1
                } else {
                    i
                };

                if s != ii {
                    args.push(packet[s..ii].to_vec());
                }
                if packet[i] == b'\n' {
                    return (args, String::default(), i + 1, true);
                }
                s = i + 1;
            } else if packet[i] == b'"' || packet[i] == b'\'' {
                let mut arg = Vec::new();
                let ch = packet[i];
                i += 1;
                s = i;
                while i < packet.len() {
                    if packet[i] == b'\n' {
                        return (
                            Vec::default(),
                            "ERR Protocol error: unbalanced quotes in request".to_string(),
                            ni,
                            false,
                        );
                    } else if packet[i] == b'\\' {
                        i += 1;
                        match packet[i] {
                            b'n' => arg.push(b'\n'),
                            b'r' => arg.push(b'\r'),
                            b't' => arg.push(b'\t'),
                            b'b' => arg.push(0x08),
                            b'a' => arg.push(0x07),
                            b'x' => {
                                let is_hex = |b: u8| -> bool {
                                    (b >= b'0' && b <= b'9')
                                        || (b >= b'a' && b <= b'f')
                                        || (b >= b'A' && b <= b'F')
                                };
                                let hex_to_digit = |b: u8| -> u8 {
                                    if b <= b'9' {
                                        b - b'0'
                                    } else if b <= b'F' {
                                        b - b'A' + 10
                                    } else {
                                        b - b'a' + 10
                                    }
                                };
                                if packet.len() - (i + 1) >= 2
                                    && is_hex(packet[i + 1])
                                    && is_hex(packet[i + 2])
                                {
                                    arg.push(
                                        hex_to_digit(packet[i + 1])
                                            << (4 + hex_to_digit(packet[i + 2])),
                                    );
                                    i += 2
                                } else {
                                    arg.push(b'x')
                                }
                            }
                            _ => arg.push(packet[i]),
                        }
                    } else if packet[i] == ch {
                        args.push(arg);
                        s = i + 1;
                        break;
                    } else {
                        arg.push(packet[i]);
                    }
                    i += 1;
                }
            }
            i += 1;
        }
        (Vec::default(), String::default(), ni, false)
    }
}
