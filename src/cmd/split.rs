use std::io;
use std::process::{Command, Stdio};

use csv;

use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
Splits the given CSV data into chunks.

The files are written to the directory given with the name '{start}.csv',
where {start} is the index of the first record of the chunk (starting at 0).

Usage:
    xsv split [options] <command> [<input>]
    xsv split --help

split options:
    -s, --size <arg>       The number of records to write into each chunk.
                           [default: 500]

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. Otherwise, the first row will
                           appear in all chunks as the header row.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Clone, Deserialize)]
struct Args {
    arg_input: Option<String>,
    arg_command: String,
    flag_size: usize,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    if args.flag_size == 0 {
        return fail!("--size must be greater than 0.");
    }
    args.sequential_split()
}

impl Args {
    fn sequential_split(&self) -> CliResult<()> {
        let rconfig = self.rconfig();
        let mut rdr = rconfig.reader()?;
        let headers = rdr.byte_headers()?.clone();

        let mut wtr = self.new_writer(&headers, &self.arg_command)?;
        let mut i = 0;
        let mut row = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut row)? {
            if i > 0 && i % self.flag_size == 0 {
                wtr.flush()?;
                wtr = self.new_writer(&headers, &self.arg_command)?;
            }
            wtr.write_byte_record(&row)?;
            i += 1;
        }
        wtr.flush()?;
        Ok(())
    }

    fn new_writer(
        &self,
        headers: &csv::ByteRecord,
        command: &str,
    ) -> CliResult<csv::Writer<Box<io::Write+'static>>> {
        let mut child = Command::new("sh")
            .args(&["-c", command])
            .stdin(Stdio::piped())
            .spawn()?;
        let stdin = Box::new(child.stdin.take().unwrap()) as Box<_>;
        let mut wtr = Config::new(&None).from_writer(stdin);
        if !self.rconfig().no_headers {
            wtr.write_record(headers)?;
        }
        Ok(wtr)
    }

    fn rconfig(&self) -> Config {
        Config::new(&self.arg_input)
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
    }
}
