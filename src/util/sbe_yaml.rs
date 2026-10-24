//! Custom transpiler from yaml to FIX SBE XML.

use std::fmt::{self, Write as _};
use std::marker::PhantomData;
use std::ops::Deref;

use indexmap::IndexMap;
use serde::Deserialize;
use serde::de::{Deserializer, Error as _, MapAccess, Visitor};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub struct Unique<V>(IndexMap<String, V>);

impl<V> Default for Unique<V> {
    fn default() -> Self {
        Self(IndexMap::new())
    }
}

impl<V> Deref for Unique<V> {
    type Target = IndexMap<String, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'de, V: Deserialize<'de>> Deserialize<'de> for Unique<V> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueVisitor<V>(PhantomData<V>);

        impl<'de, V: Deserialize<'de>> Visitor<'de> for UniqueVisitor<V> {
            type Value = Unique<V>;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a mapping with unique keys")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut entries = IndexMap::new();
                while let Some((key, value)) = map.next_entry::<String, V>()? {
                    if entries.insert(key.clone(), value).is_some() {
                        return Err(A::Error::custom(format!("duplicate key `{key}`")));
                    }
                }
                Ok(Unique(entries))
            }
        }

        deserializer.deserialize_map(UniqueVisitor(PhantomData))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    pub id: u16,
    pub version: u16,
    pub description: Option<String>,
    #[serde(default)]
    pub imports: Vec<String>,
    #[serde(default)]
    pub enums: Unique<Unique<u8>>,
    #[serde(default)]
    pub composites: Unique<Composite>,
    pub messages: Unique<Message>,
}

/// A shared YAML file contains type definitions, without a schema ID or messages.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedTypes {
    #[serde(default)]
    pub enums: Unique<Unique<u8>>,
    #[serde(default)]
    pub composites: Unique<Composite>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Composite {
    pub description: Option<String>,
    pub fields: Unique<CompositeMember>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositeMember {
    /// Only used when a message expands this composite with `fields_from`.
    pub id: Option<u16>,
    pub r#type: String,
    #[serde(default)]
    pub optional: bool,
    pub since: Option<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    pub id: u16,
    pub description: Option<String>,
    pub fields: Option<Unique<Member>>,
    /// Expand a shared composite as ordinary fields, preserving flat message APIs.
    pub fields_from: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub id: u16,
    /// Documentation only; emitted as an XML attribute without changing the wire layout.
    pub description: Option<String>,
    pub r#type: Option<String>,
    pub fields: Option<Unique<Member>>,
    #[serde(default)]
    pub optional: bool,
    pub since: Option<u16>,
}

pub fn parse(yaml: &str) -> Result<Schema, Error> {
    Ok(serde_yaml_ng::from_str(yaml)?)
}

pub fn parse_shared_types(yaml: &str) -> Result<SharedTypes, Error> {
    Ok(serde_yaml_ng::from_str(yaml)?)
}

impl Schema {
    pub fn import_types(&mut self, types: SharedTypes) -> Result<(), Error> {
        for name in types.enums.keys().chain(types.composites.keys()) {
            if self.enums.contains_key(name) || self.composites.contains_key(name) {
                return Err(format!("duplicate type `{name}`").into());
            }
        }
        self.enums.0.extend(types.enums.0);
        self.composites.0.extend(types.composites.0);
        Ok(())
    }

    pub fn to_xml(&self, package: &str, common_types: &str) -> Result<String, Error> {
        let mut xml = String::new();
        writeln!(xml, r#"<?xml version="1.0" encoding="UTF-8"?>"#)?;
        writeln!(
            xml,
            r#"<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe""#
        )?;
        writeln!(xml, r#"                   package="{}""#, escape(package))?;
        writeln!(xml, r#"                   id="{}""#, self.id)?;
        writeln!(xml, r#"                   version="{}""#, self.version)?;
        write!(xml, r#"                   byteOrder="littleEndian""#)?;
        if let Some(description) = &self.description {
            write!(
                xml,
                "\n                   description=\"{}\"",
                escape(description)
            )?;
        }
        writeln!(xml, ">")?;
        writeln!(xml, "{}", types_element(common_types)?)?;
        if !self.enums.is_empty() || !self.composites.is_empty() {
            writeln!(xml, "  <types>")?;
            for (name, values) in self.enums.iter() {
                writeln!(xml, r#"    <enum name="{name}" encodingType="uint8">"#)?;
                for (value, number) in values.iter() {
                    writeln!(
                        xml,
                        r#"      <validValue name="{value}">{number}</validValue>"#
                    )?;
                }
                writeln!(xml, "    </enum>")?;
            }
            for (name, composite) in self.composites.iter() {
                if self.enums.contains_key(name) {
                    return Err(format!("duplicate type `{name}`").into());
                }
                if composite.fields.is_empty() {
                    return Err(format!("composite `{name}` must have fields").into());
                }
                write!(xml, r#"    <composite name="{name}""#)?;
                if let Some(description) = &composite.description {
                    write!(xml, r#" description="{}""#, escape(description))?;
                }
                writeln!(xml, ">")?;
                for (field, member) in composite.fields.iter() {
                    let path = format!("{name}.{field}");
                    let (kind, presence) = composite_member_type(&path, member)?;
                    let since = since_attribute(member.since);
                    if PRIMITIVES.contains(&kind) {
                        writeln!(
                            xml,
                            r#"      <type name="{field}" primitiveType="{kind}"{presence}{since}/>"#
                        )?;
                    } else {
                        writeln!(xml, r#"      <ref name="{field}" type="{kind}"{since}/>"#)?;
                    }
                }
                writeln!(xml, "    </composite>")?;
            }
            writeln!(xml, "  </types>")?;
        }
        for (name, message) in self.messages.iter() {
            write!(xml, r#"  <sbe:message name="{name}" id="{}""#, message.id)?;
            if let Some(description) = &message.description {
                write!(xml, r#" description="{}""#, escape(description))?;
            }
            writeln!(xml, ">")?;
            match (&message.fields, &message.fields_from) {
                (Some(fields), None) => write_members(&mut xml, name, fields, 2)?,
                (None, Some(composite_name)) => {
                    let composite = self.composites.get(composite_name).ok_or_else(|| {
                        format!("{name}: unknown composite `{composite_name}` in `fields_from`")
                    })?;
                    for (field, member) in composite.fields.iter() {
                        let path = format!("{name}.{field}");
                        let id = member.id.ok_or_else(|| {
                            format!(
                                "{path}: `fields_from` requires an `id` for each composite member"
                            )
                        })?;
                        let (kind, presence) = composite_member_type(&path, member)?;
                        let since = since_attribute(member.since);
                        writeln!(
                            xml,
                            r#"    <field name="{field}" id="{id}" type="{kind}"{presence}{since}/>"#
                        )?;
                    }
                }
                _ => {
                    return Err(
                        format!("{name}: needs exactly one of `fields` or `fields_from`").into(),
                    );
                }
            }
            writeln!(xml, "  </sbe:message>")?;
        }
        writeln!(xml, "</sbe:messageSchema>")?;
        Ok(xml)
    }
}

const PRIMITIVES: &[&str] = &[
    "char", "uint8", "uint16", "uint32", "uint64", "int8", "int16", "int32", "int64", "float",
    "double",
];

fn since_attribute(version: Option<u16>) -> String {
    version
        .map(|v| format!(r#" sinceVersion="{v}""#))
        .unwrap_or_default()
}

fn composite_member_type<'a>(
    path: &str,
    member: &'a CompositeMember,
) -> Result<(&'a str, &'static str), Error> {
    if matches!(member.r#type.as_str(), "string" | "bytes") {
        return Err(format!("{path}: composites require fixed-size fields").into());
    }
    field_type(path, &member.r#type, member.optional)
}

fn field_type<'a>(
    path: &str,
    kind: &'a str,
    optional: bool,
) -> Result<(&'a str, &'static str), Error> {
    Ok(match (kind, optional) {
        ("bool", false) => ("BooleanType", ""),
        ("timestamp", false) => ("UTCTimestampMicrosEncoding", ""),
        ("timestamp", true) => ("UTCTimestampMicrosNullableEncoding", ""),
        (kind, true) if NULLABLE_PRIMITIVES.contains(&kind) => (kind, r#" presence="optional""#),
        (kind, true) => {
            return Err(format!(
                "{path}: `{kind}` has no null to spare, so `optional` would be \
             silently dropped. Enums and `bool` are already nullable — match \
             on `NullVal`; for anything else use a 0-or-1 group."
            )
            .into());
        }
        (kind, false) => (kind, ""),
    })
}

/// SBE reserves the max value of these as the null, so `optional` works as-is.
const NULLABLE_PRIMITIVES: &[&str] = &[
    "uint8", "uint16", "uint32", "uint64", "int8", "int16", "int32", "int64", "float", "double",
];

fn write_members(
    xml: &mut String,
    path: &str,
    members: &Unique<Member>,
    depth: usize,
) -> Result<(), Error> {
    let indent = "  ".repeat(depth);
    for (name, member) in members.iter() {
        let path = format!("{path}.{name}");
        let id = member.id;
        let since = since_attribute(member.since);
        let description = member
            .description
            .as_ref()
            .map(|text| format!(r#" description="{}""#, escape(text)))
            .unwrap_or_default();
        match (member.r#type.as_deref(), &member.fields) {
            (Some(_), Some(_)) | (None, None) => {
                return Err(format!("{path}: needs exactly one of `type` or `fields`").into());
            }
            (None, Some(fields)) => {
                if member.optional {
                    return Err(format!("{path}: a group cannot be `optional`").into());
                }
                writeln!(
                    xml,
                    r#"{indent}<group name="{name}" id="{id}" dimensionType="groupSizeEncoding"{since}{description}>"#
                )?;
                write_members(xml, &path, fields, depth + 1)?;
                writeln!(xml, "{indent}</group>")?;
            }
            (Some("string"), None) => {
                if member.optional {
                    return Err(format!("{path}: a string cannot be `optional`").into());
                }
                writeln!(
                    xml,
                    r#"{indent}<data name="{name}" id="{id}" type="varStringEncoding"{since}{description}/>"#
                )?;
            }
            (Some(kind), None) => {
                // `optional` is only ever a reserved value in the encoding, so it
                // is spelled differently per type: primitives get SBE's own null
                // (the type's max), a timestamp swaps to the nullable composite,
                // and anything else has nowhere to put one.
                let (kind, presence) = field_type(&path, kind, member.optional)?;
                writeln!(
                    xml,
                    r#"{indent}<field name="{name}" id="{id}" type="{kind}"{presence}{since}{description}/>"#
                )?;
            }
        }
    }
    Ok(())
}

fn types_element(xml: &str) -> Result<&str, Error> {
    let start = xml
        .find("<types>")
        .ok_or("common types must be a <types> element")?;
    let end = xml
        .rfind("</types>")
        .ok_or("common types must be a <types> element")?;
    Ok(&xml[start..end + "</types>".len()])
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
