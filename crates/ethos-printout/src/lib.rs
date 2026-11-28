//! The printout of a contract: its Signal as one Ethos file, every name it
//! imports resolved into its own types section, followed by one example
//! datom for each input and each output, generated from the types.
//!
//! Nothing here is written for a reader in Rust: the head is this crate's
//! data file, the same for every client, the prose is the authored comment
//! block that opens the contract's own file, the Ethos is ethos-zero's print
//! of the resolved file, and the examples are made from the declarations.
//! An imported name keeps the name its import gives it; a name the import
//! pulls along that the file already declares takes its crate's name before
//! it, so the printout declares every name once.

use ethos_zero::{
    Actualizing, File, Identity, Import, Name, Position, Potential, Printable, Reference, Signal,
    TypeDeclaration, Validating, Variant,
};
use std::collections::{BTreeMap, BTreeSet};

/// The prose every printout opens with, whatever its contract: how a datom
/// is written where every tool writes it alike.
pub const HEAD: &str = include_str!("../head.txt");

/// A crate a contract imports from: its Rust name, as an import's source
/// writes it, and the Ethos text of each file whose names it exports.
pub struct Origin {
    pub name: &'static str,
    pub texts: Vec<&'static str>,
}

/// A contract: its crate's Rust name, the text of its Signal file and the
/// crates it imports from.
pub struct Contract {
    pub name: &'static str,
    pub signal: &'static str,
    pub origins: Vec<Origin>,
}

/// A refusal to print: a file that would not read, or a contract whose own
/// file is not a Signal.
#[derive(Debug)]
pub enum Refusal {
    Unreadable { origin: String, error: String },
    NotSignal,
}

/// One name as a crate declares it: in which crate, under which name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Item {
    origin: usize,
    name: String,
}

/// Where a declared name stands: the file, the top-level declaration it is
/// in, and the names of the declarations around it, outermost first.
#[derive(Clone)]
struct Place {
    file: usize,
    top: usize,
    ancestors: Vec<String>,
}

/// The parts of one file a printout reads: its imports and its types.
struct Parts {
    imports: Vec<Import>,
    types: Vec<TypeDeclaration>,
}

/// A crate read: its name, its files' parts and every name they declare.
struct Read {
    name: String,
    files: Vec<Parts>,
    declared: BTreeMap<String, Place>,
}

/// A contract read whole: crate 0 is the contract itself.
struct Reading {
    name: String,
    origins: Vec<Read>,
    inputs: Vec<Variant>,
    outputs: Vec<Variant>,
    prose: String,
}

/// The printout, made once: its text and the example of each input and output.
pub struct Printout {
    resolved: Signal,
    prose: String,
}

/// The printing of a contract.
pub trait Printing {
    fn printout(&self) -> Result<Printout, Refusal>;
}

/// The reading of a contract's files.
trait Reads {
    fn read(&self) -> Result<Reading, Refusal>;
    fn prose(&self) -> String;
}

impl Printing for Contract {
    fn printout(&self) -> Result<Printout, Refusal> {
        let reading = self.read()?;
        let resolved = reading.resolve();
        Ok(Printout {
            resolved,
            prose: reading.prose,
        })
    }
}

impl Reads for Contract {
    fn read(&self) -> Result<Reading, Refusal> {
        let own = Potential::<File>::from(self.signal)
            .actualize()
            .map_err(|error| Refusal::Unreadable {
                origin: "signal".into(),
                error: format!("{error:?}"),
            })?;
        let File::Signal(signal) = own else {
            return Err(Refusal::NotSignal);
        };
        let mut origins = vec![Read::of(
            "crate".into(),
            vec![Parts {
                imports: signal.imports,
                types: signal.types,
            }],
            &[&signal.inputs, &signal.outputs],
        )];
        for origin in &self.origins {
            let mut files = Vec::new();
            for text in &origin.texts {
                let file = Potential::<File>::from(*text)
                    .actualize()
                    .map_err(|error| Refusal::Unreadable {
                        origin: origin.name.into(),
                        error: format!("{error:?}"),
                    })?;
                files.push(Parts::of(file));
            }
            origins.push(Read::of(origin.name.into(), files, &[]));
        }
        Ok(Reading {
            name: self.name.into(),
            origins,
            inputs: signal.inputs,
            outputs: signal.outputs,
            prose: self.prose(),
        })
    }

    /// The comment block the file opens with, before its root's head.
    fn prose(&self) -> String {
        let mut prose = String::new();
        for line in self.signal.lines() {
            let trimmed = line.trim_start();
            if !(trimmed.is_empty() || trimmed.starts_with(';')) {
                break;
            }
            prose.push_str(line);
            prose.push('\n');
        }
        prose
    }
}

trait ReadsParts {
    fn of(file: File) -> Self;
}

impl ReadsParts for Parts {
    fn of(file: File) -> Self {
        match file {
            File::Library(library) => Self {
                imports: library.imports,
                types: library.types,
            },
            File::Signal(signal) => Self {
                imports: signal.imports,
                types: signal.types,
            },
            File::Operation(operation) => Self {
                imports: operation.imports,
                types: operation.types,
            },
            File::Memory(memory) => Self {
                imports: memory.imports,
                types: memory.types,
            },
            File::Emission(emission) => Self {
                imports: emission.imports,
                types: emission.types,
            },
        }
    }
}

trait ReadsCrate {
    fn of(name: String, files: Vec<Parts>, variants: &[&Vec<Variant>]) -> Self;
}

impl ReadsCrate for Read {
    fn of(name: String, files: Vec<Parts>, variants: &[&Vec<Variant>]) -> Self {
        let mut declared = BTreeMap::new();
        for (file, parts) in files.iter().enumerate() {
            for (top, declaration) in parts.types.iter().enumerate() {
                let place = Place {
                    file,
                    top,
                    ancestors: vec![],
                };
                declaration.declare(&place, &mut declared);
            }
        }
        // The names the inputs and outputs declare in place are the file's
        // own; they are never moved, so they stand at no top declaration.
        for section in variants {
            for variant in section.iter() {
                let place = Place {
                    file: usize::MAX,
                    top: usize::MAX,
                    ancestors: vec![],
                };
                variant.declare(&place, &mut declared);
            }
        }
        Self {
            name,
            files,
            declared,
        }
    }
}

trait Declares {
    /// Record every type name this declares, at the place it stands.
    fn declare(&self, place: &Place, declared: &mut BTreeMap<String, Place>);
}

impl Declares for TypeDeclaration {
    fn declare(&self, place: &Place, declared: &mut BTreeMap<String, Place>) {
        let name = self.identity().name.as_ref().to_owned();
        declared.insert(name.clone(), place.clone());
        let mut inner = place.clone();
        inner.ancestors.push(name);
        match self {
            TypeDeclaration::Struct(_, positions) => {
                for position in positions {
                    position.declare(&inner, declared);
                }
            }
            TypeDeclaration::Enum(_, variants) => {
                for variant in variants {
                    variant.declare(&inner, declared);
                }
            }
            TypeDeclaration::Alias(..) => {}
        }
    }
}

impl Declares for Position {
    fn declare(&self, place: &Place, declared: &mut BTreeMap<String, Place>) {
        if let Position::Declared(declaration) = self {
            declaration.declare(place, declared);
        }
    }
}

impl Declares for Variant {
    fn declare(&self, place: &Place, declared: &mut BTreeMap<String, Place>) {
        match self {
            Variant::Bare(_) | Variant::Typed(..) => {}
            Variant::Struct(_, positions) => {
                for position in positions {
                    position.declare(place, declared);
                }
            }
            Variant::Enum(name, variants) => {
                declared.insert(name.as_ref().to_owned(), place.clone());
                let mut inner = place.clone();
                inner.ancestors.push(name.as_ref().to_owned());
                for variant in variants {
                    variant.declare(&inner, declared);
                }
            }
        }
    }
}

trait Identified {
    fn identity(&self) -> &Identity;
    fn identity_mut(&mut self) -> &mut Identity;
}

impl Identified for TypeDeclaration {
    fn identity(&self) -> &Identity {
        match self {
            TypeDeclaration::Struct(identity, _)
            | TypeDeclaration::Enum(identity, _)
            | TypeDeclaration::Alias(identity, _) => identity,
        }
    }
    fn identity_mut(&mut self) -> &mut Identity {
        match self {
            TypeDeclaration::Struct(identity, _)
            | TypeDeclaration::Enum(identity, _)
            | TypeDeclaration::Alias(identity, _) => identity,
        }
    }
}

/// A walk over every reference and every declared name in a part of a file.
trait Walked {
    fn references(&self, each: &mut dyn FnMut(&Reference, bool));
    fn rename(&mut self, names: &mut dyn FnMut(&str, bool) -> Option<String>);
}

impl Walked for Reference {
    fn references(&self, each: &mut dyn FnMut(&Reference, bool)) {
        each(self, false);
        for argument in &self.arguments {
            argument.references(each);
        }
    }
    fn rename(&mut self, names: &mut dyn FnMut(&str, bool) -> Option<String>) {
        // A reference is rewritten by the reference itself: the closure is
        // handed its whole text, its source before its name.
        let written = match &self.source {
            Some(source) => format!("{}::{}", source.as_ref(), self.name.as_ref()),
            None => self.name.as_ref().to_owned(),
        };
        if let Some(name) = names(&written, false) {
            self.source = None;
            self.name = Name::try_from(name.as_str()).expect("a resolved name is a name");
        }
        for argument in &mut self.arguments {
            argument.rename(names);
        }
    }
}

impl Walked for Position {
    fn references(&self, each: &mut dyn FnMut(&Reference, bool)) {
        match self {
            Position::Referenced(reference) => reference.references(each),
            Position::Declared(declaration) => declaration.references(each),
        }
    }
    fn rename(&mut self, names: &mut dyn FnMut(&str, bool) -> Option<String>) {
        match self {
            Position::Referenced(reference) => reference.rename(names),
            Position::Declared(declaration) => declaration.rename(names),
        }
    }
}

impl Walked for Variant {
    fn references(&self, each: &mut dyn FnMut(&Reference, bool)) {
        match self {
            // A bare variant naming a type carries that type.
            Variant::Bare(name) => each(
                &Reference {
                    source: None,
                    name: name.clone(),
                    arguments: vec![],
                },
                true,
            ),
            Variant::Typed(_, reference) => reference.references(each),
            Variant::Struct(_, positions) => {
                for position in positions {
                    position.references(each);
                }
            }
            Variant::Enum(_, variants) => {
                for variant in variants {
                    variant.references(each);
                }
            }
        }
    }
    fn rename(&mut self, names: &mut dyn FnMut(&str, bool) -> Option<String>) {
        match self {
            Variant::Bare(_) => {}
            Variant::Typed(_, reference) => reference.rename(names),
            Variant::Struct(_, positions) => {
                for position in positions {
                    position.rename(names);
                }
            }
            // A nested enum's name is its variant's, written on the wire.
            Variant::Enum(_, variants) => {
                for variant in variants {
                    variant.rename(names);
                }
            }
        }
    }
}

impl Walked for TypeDeclaration {
    fn references(&self, each: &mut dyn FnMut(&Reference, bool)) {
        match self {
            TypeDeclaration::Struct(_, positions) => {
                for position in positions {
                    position.references(each);
                }
            }
            TypeDeclaration::Enum(_, variants) => {
                for variant in variants {
                    variant.references(each);
                }
            }
            TypeDeclaration::Alias(_, reference) => reference.references(each),
        }
    }
    fn rename(&mut self, names: &mut dyn FnMut(&str, bool) -> Option<String>) {
        let identity = self.identity_mut();
        if let Some(renamed) = names(identity.name.as_ref(), true) {
            identity.name = Name::try_from(renamed.as_str()).expect("a resolved name is a name");
        }
        match self {
            TypeDeclaration::Struct(_, positions) => {
                for position in positions {
                    position.rename(names);
                }
            }
            TypeDeclaration::Enum(_, variants) => {
                for variant in variants {
                    variant.rename(names);
                }
            }
            TypeDeclaration::Alias(_, reference) => reference.rename(names),
        }
    }
}

/// The resolution of a contract's names across the crates it imports from.
trait Resolves {
    fn origin_of(&self, from: usize, source: &str) -> Option<usize>;
    fn find(&self, origin: usize, name: &str) -> Option<Item>;
    fn find_bare(&self, origin: usize, file: usize, name: &str) -> Option<Item>;
    fn find_reference(&self, origin: usize, written: &str) -> Option<Item>;
    fn declaration(&self, item: &Item) -> Option<&TypeDeclaration>;
    fn resolve(&self) -> Signal;
}

impl Resolves for Reading {
    /// The crate an import's source names, seen from a crate.
    fn origin_of(&self, from: usize, source: &str) -> Option<usize> {
        let source = source.trim_start_matches("::");
        if source == "crate" {
            return Some(from);
        }
        self.origins.iter().position(|read| read.name == source)
    }

    /// The declaration a name in a crate stands for: its own, or the one
    /// its import of that name stands for. None for an intrinsic.
    fn find(&self, origin: usize, name: &str) -> Option<Item> {
        let read = &self.origins[origin];
        if read.declared.contains_key(name) {
            return Some(Item {
                origin,
                name: name.into(),
            });
        }
        for parts in &read.files {
            for import in &parts.imports {
                let (source, imported) = match import {
                    Import::One(source, imported) => (source, std::slice::from_ref(imported)),
                    Import::Many(source, imported) => (source, imported.as_slice()),
                };
                for one in imported {
                    if one.name.as_ref() == name {
                        let target = self.origin_of(origin, source.as_ref())?;
                        return self.find(target, one.emitted.as_ref());
                    }
                }
            }
        }
        None
    }

    /// The type a bare variant in a file names: one the file declares or
    /// imports, as ethos-zero reads one file. None when it carries nothing.
    fn find_bare(&self, origin: usize, file: usize, name: &str) -> Option<Item> {
        let read = &self.origins[origin];
        if let Some(place) = read.declared.get(name)
            && (origin == 0 || place.file == file)
        {
            return Some(Item {
                origin,
                name: name.into(),
            });
        }
        let parts = read.files.get(if origin == 0 { 0 } else { file })?;
        for import in &parts.imports {
            let (source, imported) = match import {
                Import::One(source, imported) => (source, std::slice::from_ref(imported)),
                Import::Many(source, imported) => (source, imported.as_slice()),
            };
            for one in imported {
                if one.name.as_ref() == name {
                    let target = self.origin_of(origin, source.as_ref())?;
                    return self.find(target, one.emitted.as_ref());
                }
            }
        }
        None
    }

    /// What a reference, written in a crate, stands for.
    fn find_reference(&self, origin: usize, written: &str) -> Option<Item> {
        match written.rsplit_once("::") {
            Some((source, name)) => self.find(self.origin_of(origin, source)?, name),
            None => self.find(origin, written),
        }
    }

    /// The declaration an item names, the outermost one around it whose
    /// subtree holds it.
    fn declaration(&self, item: &Item) -> Option<&TypeDeclaration> {
        let read = &self.origins[item.origin];
        let place = read.declared.get(&item.name)?;
        let top = read.files.get(place.file)?.types.get(place.top)?;
        top.find_named(&item.name)
    }

    fn resolve(&self) -> Signal {
        // Every item the contract reaches outside itself, in the order reached.
        let mut needed: Vec<Item> = Vec::new();
        let mut seen: BTreeSet<Item> = BTreeSet::new();
        let mut queue: Vec<Item> = Vec::new();
        // The names of variants, by the crates they stand in: of a bare one
        // that carries nothing, and of one that carries something. A type
        // printed under one beside a variant of another crate would be
        // carried by it, or clash with its payload, so it takes a prefix.
        let mut units: BTreeMap<String, BTreeSet<(usize, usize)>> = BTreeMap::new();
        let reach = |origin: usize,
                     file: usize,
                     reference: &Reference,
                     bare: bool,
                     queue: &mut Vec<Item>,
                     units: &mut BTreeMap<String, BTreeSet<(usize, usize)>>| {
            let written = match &reference.source {
                Some(source) => format!("{}::{}", source.as_ref(), reference.name.as_ref()),
                None => reference.name.as_ref().to_owned(),
            };
            let found = if bare {
                self.find_bare(origin, file, &written)
            } else {
                self.find_reference(origin, &written)
            };
            match found {
                Some(item) if item.origin != 0 => queue.push(item),
                Some(_) => {}
                None if bare => {
                    units.entry(written).or_default().insert((origin, file));
                }
                None => {}
            }
        };
        for variant in self.inputs.iter().chain(&self.outputs) {
            variant.references(&mut |reference, bare| {
                reach(0, 0, reference, bare, &mut queue, &mut units)
            });
        }
        for declaration in &self.origins[0].files[0].types {
            declaration.references(&mut |reference, bare| {
                reach(0, 0, reference, bare, &mut queue, &mut units)
            });
            declaration.variant_names(&mut |name| {
                units.entry(name.into()).or_default().insert((0, 0));
            });
        }
        for variant in self.inputs.iter().chain(&self.outputs) {
            variant.variant_names(&mut |name| {
                units.entry(name.into()).or_default().insert((0, 0));
            });
        }
        let mut cursor = 0;
        while cursor < queue.len() {
            let item = queue[cursor].clone();
            cursor += 1;
            if !seen.insert(item.clone()) {
                continue;
            }
            if let Some(declaration) = self.declaration(&item) {
                let file = self.origins[item.origin].declared[&item.name].file;
                let mut reached = Vec::new();
                declaration.references(&mut |reference, bare| {
                    reach(item.origin, file, reference, bare, &mut reached, &mut units)
                });
                declaration.variant_names(&mut |name| {
                    units
                        .entry(name.into())
                        .or_default()
                        .insert((item.origin, file));
                });
                queue.extend(reached);
            }
            needed.push(item);
        }
        // Every name a needed declaration declares in place is printed with it.
        let mut carried: BTreeSet<Item> = BTreeSet::new();
        for item in &needed {
            let read = &self.origins[item.origin];
            for (name, place) in &read.declared {
                if read.declared.get(&item.name).is_some_and(|own| {
                    own.file == place.file
                        && own.top == place.top
                        && place.ancestors.contains(&item.name)
                }) {
                    carried.insert(Item {
                        origin: item.origin,
                        name: name.clone(),
                    });
                }
            }
        }
        // The printed names: the contract's own, then each import's, then
        // the rest, the crate's name before one taken or clashing.
        let mut printed: BTreeMap<Item, String> = BTreeMap::new();
        let mut taken: BTreeSet<String> = BTreeSet::new();
        // A clash is with a variant of another file: ethos-zero reads a
        // variant's name against its own file's types alone.
        let clashes = |item: &Item, name: &str, taken: &BTreeSet<String>| {
            let file = match item.origin {
                0 => 0,
                origin => self.origins[origin].declared[&item.name].file,
            };
            taken.contains(name)
                || units
                    .get(name)
                    .is_some_and(|files| files.iter().any(|other| *other != (item.origin, file)))
        };
        let mut wire: BTreeSet<String> = BTreeSet::new();
        for variant in self.inputs.iter().chain(&self.outputs) {
            variant.enum_names(&mut |name| {
                wire.insert(name.into());
            });
        }
        for declaration in &self.origins[0].files[0].types {
            declaration.enum_names(&mut |name| {
                wire.insert(name.into());
            });
        }
        for name in self.origins[0].declared.keys() {
            let own = Item {
                origin: 0,
                name: name.clone(),
            };
            if !wire.contains(name) && clashes(&own, name, &taken) {
                let prefixed = format!("{}{}", self.name.pascal(), name);
                taken.insert(prefixed.clone());
                printed.insert(
                    Item {
                        origin: 0,
                        name: name.clone(),
                    },
                    prefixed,
                );
            } else {
                taken.insert(name.clone());
            }
        }
        for parts in &self.origins[0].files {
            for import in &parts.imports {
                let (source, imported) = match import {
                    Import::One(source, imported) => (source, std::slice::from_ref(imported)),
                    Import::Many(source, imported) => (source, imported.as_slice()),
                };
                for one in imported {
                    let Some(target) = self.origin_of(0, source.as_ref()) else {
                        continue;
                    };
                    if let Some(item) = self.find(target, one.emitted.as_ref())
                        && !printed.contains_key(&item)
                        && !clashes(&item, one.name.as_ref(), &taken)
                    {
                        taken.insert(one.name.as_ref().to_owned());
                        printed.insert(item, one.name.as_ref().to_owned());
                    }
                }
            }
        }
        for item in needed.iter().chain(&carried) {
            if printed.contains_key(item) {
                continue;
            }
            let mut name = item.name.clone();
            if clashes(item, &name, &taken) {
                name = format!("{}{}", self.origins[item.origin].name.pascal(), item.name);
            }
            taken.insert(name.clone());
            printed.insert(item.clone(), name);
        }
        let rename = |origin: usize, written: &str, declaring: bool| -> Option<String> {
            let item = if declaring {
                Item {
                    origin,
                    name: written.into(),
                }
            } else {
                self.find_reference(origin, written)?
            };
            match printed.get(&item) {
                Some(name) => Some(name.clone()),
                None if item.origin == 0 => Some(item.name),
                None => None,
            }
        };
        let mut inputs = self.inputs.clone();
        let mut outputs = self.outputs.clone();
        for variant in inputs.iter_mut().chain(outputs.iter_mut()) {
            variant.rename(&mut |written, declaring| rename(0, written, declaring));
        }
        let mut types = self.origins[0].files[0].types.clone();
        for declaration in &mut types {
            declaration.rename(&mut |written, declaring| rename(0, written, declaring));
        }
        for item in &needed {
            if carried.contains(item) {
                continue;
            }
            if let Some(declaration) = self.declaration(item) {
                let mut declaration = declaration.clone();
                declaration
                    .rename(&mut |written, declaring| rename(item.origin, written, declaring));
                types.push(declaration);
            }
        }
        Signal {
            imports: vec![],
            inputs,
            outputs,
            types,
        }
    }
}

/// A walk over the names of the variants that carry something, and of
/// every variant of a nested enum.
trait VariantNames {
    fn variant_names(&self, each: &mut dyn FnMut(&str));
}

impl VariantNames for Variant {
    fn variant_names(&self, each: &mut dyn FnMut(&str)) {
        match self {
            Variant::Bare(_) => {}
            Variant::Typed(name, _) => each(name.as_ref()),
            Variant::Struct(name, positions) => {
                each(name.as_ref());
                for position in positions {
                    position.variant_names(each);
                }
            }
            Variant::Enum(name, variants) => {
                each(name.as_ref());
                for variant in variants {
                    variant.variant_names(each);
                }
            }
        }
    }
}

impl VariantNames for Position {
    fn variant_names(&self, each: &mut dyn FnMut(&str)) {
        if let Position::Declared(declaration) = self {
            declaration.variant_names(each);
        }
    }
}

impl VariantNames for TypeDeclaration {
    fn variant_names(&self, each: &mut dyn FnMut(&str)) {
        match self {
            TypeDeclaration::Struct(_, positions) => {
                for position in positions {
                    position.variant_names(each);
                }
            }
            TypeDeclaration::Enum(_, variants) => {
                for variant in variants {
                    variant.variant_names(each);
                }
            }
            TypeDeclaration::Alias(..) => {}
        }
    }
}

/// A walk over the names of nested enums declared by variants, which are
/// variant names too and never renamed.
trait EnumNames {
    fn enum_names(&self, each: &mut dyn FnMut(&str));
}

impl EnumNames for Variant {
    fn enum_names(&self, each: &mut dyn FnMut(&str)) {
        match self {
            Variant::Bare(_) | Variant::Typed(..) => {}
            Variant::Struct(_, positions) => {
                for position in positions {
                    if let Position::Declared(declaration) = position {
                        declaration.enum_names(each);
                    }
                }
            }
            Variant::Enum(name, variants) => {
                each(name.as_ref());
                for variant in variants {
                    variant.enum_names(each);
                }
            }
        }
    }
}

impl EnumNames for TypeDeclaration {
    fn enum_names(&self, each: &mut dyn FnMut(&str)) {
        match self {
            TypeDeclaration::Struct(_, positions) => {
                for position in positions {
                    if let Position::Declared(declaration) = position {
                        declaration.enum_names(each);
                    }
                }
            }
            TypeDeclaration::Enum(_, variants) => {
                for variant in variants {
                    variant.enum_names(each);
                }
            }
            TypeDeclaration::Alias(..) => {}
        }
    }
}

trait FindsNamed {
    fn find_named(&self, name: &str) -> Option<&TypeDeclaration>;
}

impl FindsNamed for TypeDeclaration {
    fn find_named(&self, name: &str) -> Option<&TypeDeclaration> {
        if self.identity().name.as_ref() == name {
            return Some(self);
        }
        match self {
            TypeDeclaration::Struct(_, positions) => {
                for position in positions {
                    if let Position::Declared(inner) = position
                        && let Some(found) = inner.find_named(name)
                    {
                        return Some(found);
                    }
                }
                None
            }
            TypeDeclaration::Enum(_, variants) => {
                for variant in variants {
                    if let Some(found) = variant.find_named(name) {
                        return Some(found);
                    }
                }
                None
            }
            TypeDeclaration::Alias(..) => None,
        }
    }
}

impl FindsNamed for Variant {
    fn find_named(&self, name: &str) -> Option<&TypeDeclaration> {
        match self {
            Variant::Struct(_, positions) => {
                for position in positions {
                    if let Position::Declared(inner) = position
                        && let Some(found) = inner.find_named(name)
                    {
                        return Some(found);
                    }
                }
                None
            }
            Variant::Enum(_, variants) => {
                for variant in variants {
                    if let Some(found) = variant.find_named(name) {
                        return Some(found);
                    }
                }
                None
            }
            _ => None,
        }
    }
}

trait Pascal {
    fn pascal(&self) -> String;
}

impl Pascal for String {
    fn pascal(&self) -> String {
        let mut pascal = String::new();
        for segment in self.split(['_', ':']) {
            let mut characters = segment.chars();
            if let Some(first) = characters.next() {
                pascal.extend(first.to_uppercase());
                pascal.push_str(characters.as_str());
            }
        }
        pascal
    }
}

/// How deep an example opens a vector or picks a variant that carries
/// something before it stops at the smallest value.
const EXAMPLE_DEPTH: usize = 6;

/// The printout's text, its check and its examples.
pub trait Printed {
    fn text(&self) -> String;
    fn check(&self) -> Result<(), String>;
    fn input_examples(&self) -> Vec<String>;
    fn output_examples(&self) -> Vec<String>;
}

trait Exemplifies {
    fn examples(&self, variants: &[Variant]) -> Vec<String>;
}

impl Printed for Printout {
    /// The whole printout: the head, the prose, the resolved Ethos, then one
    /// example per input and per output as comments, inputs first.
    fn text(&self) -> String {
        let mut text = HEAD.to_owned();
        text.push_str(&self.prose);
        text.push_str(&File::Signal(self.resolved.clone()).print());
        for section in [self.input_examples(), self.output_examples()] {
            text.push('\n');
            for example in section {
                text.push_str("; ");
                text.push_str(&example);
                text.push('\n');
            }
        }
        text
    }

    /// The printed text, read back by ethos-zero as it stands and checked
    /// whole: the error that refuses it, if any.
    fn check(&self) -> Result<(), String> {
        File::Signal(self.resolved.clone())
            .validate()
            .map_err(|error| format!("{error:?}"))?;
        Potential::<File>::from(self.text())
            .actualize()
            .map_err(|error| format!("{error:?}"))?
            .validate()
            .map_err(|error| format!("{error:?}"))
    }

    fn input_examples(&self) -> Vec<String> {
        self.examples(&self.resolved.inputs)
    }

    fn output_examples(&self) -> Vec<String> {
        self.examples(&self.resolved.outputs)
    }
}

impl Exemplifies for Printout {
    fn examples(&self, variants: &[Variant]) -> Vec<String> {
        let mut declarations = BTreeMap::new();
        for declaration in &self.resolved.types {
            declaration.index(&mut declarations);
        }
        for variant in self.resolved.inputs.iter().chain(&self.resolved.outputs) {
            variant.index(&mut declarations);
        }
        let examples = Examples { declarations };
        variants
            .iter()
            .map(|variant| examples.variant(variant, 0))
            .collect()
    }
}

trait Indexed {
    fn index<'a>(&'a self, declarations: &mut BTreeMap<String, &'a TypeDeclaration>);
}

impl Indexed for TypeDeclaration {
    fn index<'a>(&'a self, declarations: &mut BTreeMap<String, &'a TypeDeclaration>) {
        declarations.insert(self.identity().name.as_ref().to_owned(), self);
        match self {
            TypeDeclaration::Struct(_, positions) => {
                for position in positions {
                    if let Position::Declared(inner) = position {
                        inner.index(declarations);
                    }
                }
            }
            TypeDeclaration::Enum(_, variants) => {
                for variant in variants {
                    variant.index(declarations);
                }
            }
            TypeDeclaration::Alias(..) => {}
        }
    }
}

impl Indexed for Variant {
    fn index<'a>(&'a self, declarations: &mut BTreeMap<String, &'a TypeDeclaration>) {
        match self {
            Variant::Struct(_, positions) => {
                for position in positions {
                    if let Position::Declared(inner) = position {
                        inner.index(declarations);
                    }
                }
            }
            Variant::Enum(_, variants) => {
                for variant in variants {
                    variant.index(declarations);
                }
            }
            _ => {}
        }
    }
}

/// The maker of example datoms over a resolved file's declarations.
struct Examples<'a> {
    declarations: BTreeMap<String, &'a TypeDeclaration>,
}

trait Exemplifying {
    fn variant(&self, variant: &Variant, depth: usize) -> String;
    fn choice(&self, variants: &[Variant], depth: usize) -> String;
    fn positions(&self, positions: &[Position], depth: usize) -> String;
    fn declaration(&self, declaration: &TypeDeclaration, depth: usize) -> String;
    fn reference(&self, reference: &Reference, holder: Option<&str>, depth: usize) -> String;
}

impl Exemplifying for Examples<'_> {
    fn variant(&self, variant: &Variant, depth: usize) -> String {
        match variant {
            Variant::Bare(name) => match self.declarations.get(name.as_ref()) {
                Some(declaration) => {
                    format!(
                        "{}.{}",
                        name.as_ref(),
                        self.declaration(declaration, depth + 1)
                    )
                }
                None => name.as_ref().to_owned(),
            },
            Variant::Typed(name, reference) => {
                format!(
                    "{}.{}",
                    name.as_ref(),
                    self.reference(reference, None, depth + 1)
                )
            }
            Variant::Struct(name, positions) => {
                format!("{}.{}", name.as_ref(), self.positions(positions, depth + 1))
            }
            Variant::Enum(name, variants) => {
                format!("{}.{}", name.as_ref(), self.choice(variants, depth + 1))
            }
        }
    }

    /// The variant an example takes: the first, or past the depth the
    /// first that carries nothing.
    fn choice(&self, variants: &[Variant], depth: usize) -> String {
        let chosen = if depth >= EXAMPLE_DEPTH {
            variants
                .iter()
                .find(|variant| matches!(variant, Variant::Bare(_)))
                .or(variants.first())
        } else {
            variants.first()
        };
        match chosen {
            Some(variant) => self.variant(variant, depth),
            None => String::new(),
        }
    }

    fn positions(&self, positions: &[Position], depth: usize) -> String {
        if positions.is_empty() {
            return "{}".into();
        }
        let inner: Vec<String> = positions
            .iter()
            .map(|position| match position {
                Position::Referenced(reference) => self.reference(reference, None, depth),
                Position::Declared(declaration) => self.declaration(declaration, depth),
            })
            .collect();
        format!("{{ {} }}", inner.join(" "))
    }

    fn declaration(&self, declaration: &TypeDeclaration, depth: usize) -> String {
        match declaration {
            TypeDeclaration::Struct(_, positions) => self.positions(positions, depth + 1),
            TypeDeclaration::Enum(_, variants) => self.choice(variants, depth + 1),
            TypeDeclaration::Alias(identity, reference) => {
                self.reference(reference, Some(identity.name.as_ref()), depth)
            }
        }
    }

    /// A value of a referenced type; a string holds the name of the type
    /// that holds it.
    fn reference(&self, reference: &Reference, holder: Option<&str>, depth: usize) -> String {
        let name = reference.name.as_ref();
        match name {
            "String" => format!("«{}»", holder.unwrap_or(name)),
            "Integer" => "0".into(),
            "Decimal" => "0.0".into(),
            "Boolean" => "False".into(),
            "Option" => "None".into(),
            "Vector" => match reference.arguments.first() {
                Some(argument) if depth < EXAMPLE_DEPTH => {
                    format!("[ {} ]", self.reference(argument, None, depth + 1))
                }
                _ => "[]".into(),
            },
            _ => match self.declarations.get(name) {
                Some(declaration) => self.declaration(declaration, depth),
                None => name.to_owned(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Contract, HEAD, Origin, Printed, Printing};

    /// A name imported along with its neighbours, and a unit variant of
    /// another file whose name a carried type shares: the printout declares
    /// the import, keeps the unit a unit, and checks.
    #[test]
    fn imports_resolve_and_a_unit_stays_a_unit() {
        let contract = Contract {
            name: "fixture",
            signal: "; The fixture's prose.\nSignal\n[ library:[ Holder ] ]\n[ Put.Holder\n  Ask ]\n[ Taken.Queue\n  Asked ]\n[ Queue.{ String } ]\n",
            origins: vec![Origin {
                name: "library",
                texts: vec![
                    "Library\n[ crate:Delivery ]\n[ Holder.{ Inner.Vector<String> Delivery } ]\n[]\n[]\n",
                    "Library\n[]\n[ Delivery.[ Queue Wake ] ]\n[]\n[]\n",
                ],
            }],
        };
        let printout = contract.printout().expect("the fixture reads");
        printout.check().expect("the printout checks");
        let text = printout.text();
        assert!(
            text.strip_prefix(HEAD)
                .is_some_and(|rest| rest.starts_with("; The fixture's prose.\nSignal\n")),
            "{text}"
        );
        assert!(text.contains("FixtureQueue"), "{text}");
        assert_eq!(
            printout.input_examples(),
            ["Put.{ [ «String» ] Queue }", "Ask"]
        );
        assert_eq!(printout.output_examples(), ["Taken.{ «String» }", "Asked"]);
    }
}
