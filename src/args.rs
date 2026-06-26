use clap::Parser;

#[derive(Parser)]
pub struct Args {
    #[arg(short, long)]
    pub config: String,
}
