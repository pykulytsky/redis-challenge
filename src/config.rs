use clap::Parser;

#[derive(Debug, Parser, Clone)]
pub struct Config {
    #[arg(short, long)]
    pub dir: Option<String>,

    #[arg(short, long)]
    pub dbfilename: Option<String>,
}
