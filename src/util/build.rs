//! Build helpers to auto-generate the rust code for the SBE schemas.

pub mod sbe_yaml;

use std::fs;

use ergo_sbe::miette::{IntoDiagnostic, Result, miette};
use ergo_sbe::{ConversionSelector, DomainVarData, GenerationConfig, generate_str_to_out_dir};

const SCHEMA: &str = "sbe_schemas/engineering_challenge/challenge_orderbook.yaml";
const COMMON_TYPES: &str = "sbe_schemas/includes/common-types.xml";

fn main() -> Result<()> {
    println!("cargo::rerun-if-changed={SCHEMA}");
    println!("cargo::rerun-if-changed={COMMON_TYPES}");

    let yaml = fs::read_to_string(SCHEMA).into_diagnostic()?;
    let schema = sbe_yaml::parse(&yaml).map_err(|error| miette!("{error}"))?;
    let common_types = fs::read_to_string(COMMON_TYPES).into_diagnostic()?;
    let xml = schema
        .to_xml("challenge_orderbook_messages", &common_types)
        .map_err(|error| miette!("{error}"))?;

    let config = GenerationConfig::new("challenge_orderbook")
        .with_domain_objects(DomainVarData::Strings)
        .with_domain_type(
            ConversionSelector::named_type("UTCTimestampMicrosEncoding"),
            "chrono::DateTime<chrono::Utc>",
        )
        .with_domain_type(
            ConversionSelector::named_type("SideEncoding"),
            "crate::Side",
        );
    generate_str_to_out_dir(&xml, config)?;
    Ok(())
}
