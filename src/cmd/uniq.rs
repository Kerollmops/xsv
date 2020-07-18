use csv;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use CliResult;
use config::{Delimiter, Config};
use select::SelectColumns;
use util;

static USAGE: &'static str = "
Omit repeated records among multiple CSV files.

Usage:
    xsv uniq [options] [<input>...]

options:
    -s, --select <arg>     Select the columns to deduplicate.
                           See 'xsv select -h' for the full syntax.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Vec<String>,
    flag_output: Option<String>,
    flag_select: SelectColumns,
    flag_delimiter: Option<Delimiter>,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let configs = util::many_configs(&args.arg_input,
                           args.flag_delimiter,
                           false)?;

    let mut hashes = HashSet::new();
    let mut buffer = Vec::new();
    let mut wtr = Config::new(&args.flag_output).writer()?;
    let mut record = csv::ByteRecord::new();
    for (i, conf) in configs.into_iter().enumerate() {
        let conf = conf.select(args.flag_select.clone());
        let mut rdr = conf.reader()?;
        let headers = rdr.headers()?;
        if i == 0 {
            wtr.write_byte_record(headers.as_byte_record())?;
        }
        let sel = conf.selection(headers.as_byte_record())?;
        while rdr.read_byte_record(&mut record)? {
            buffer.clear();
            for f in sel.select(&record) {
                buffer.extend_from_slice(f);
                buffer.push(0);
            }

            let hash = calculate_hash(&buffer);
            if hashes.insert(hash) {
                wtr.write_byte_record(&record)?;
            }
        }
    }
    wtr.flush().map_err(From::from)
}
