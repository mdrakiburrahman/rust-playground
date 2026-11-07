use chumsky::{Stream, prelude::*};

use crate::{Column, CreateNamespace, CreateTable, DataType, ParseError, Statement, TableMetadata};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Token {
    Word(String),
    Ident(String),
    String(String),
    Number(String),
    Symbol(char),
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Debug)]
struct SqlError(Box<Simple<Token>>);

impl SqlError {
    fn custom(span: Range<usize>, message: impl ToString) -> Self {
        Self(Box::new(Simple::custom(span, message)))
    }

    fn span(&self) -> Range<usize> {
        self.0.span()
    }
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl chumsky::Error<Token> for SqlError {
    type Span = Range<usize>;
    type Label = &'static str;

    fn expected_input_found<I: IntoIterator<Item = Option<Token>>>(
        span: Self::Span,
        expected: I,
        found: Option<Token>,
    ) -> Self {
        Self(Box::new(Simple::expected_input_found(span, expected, found)))
    }

    fn with_label(self, label: Self::Label) -> Self {
        Self(Box::new(self.0.with_label(label)))
    }

    fn merge(self, other: Self) -> Self {
        Self(Box::new(self.0.merge(*other.0)))
    }
}

type P<T> = BoxedParser<'static, Token, T, SqlError>;

fn kw(word: &'static str) -> P<()> {
    filter(move |token: &Token| matches!(token, Token::Word(value) if value.eq_ignore_ascii_case(word)))
        .ignored()
        .boxed()
}

fn sym(symbol: char) -> P<()> {
    just(Token::Symbol(symbol)).ignored().boxed()
}

fn ident() -> P<String> {
    select! { Token::Word(value) => value.to_ascii_lowercase(), Token::Ident(value) => value }
        .try_map(|value, span| {
            if value.is_empty() {
                Err(SqlError::custom(span, "empty identifier"))
            } else {
                Ok(value)
            }
        })
        .boxed()
}

fn string() -> P<String> {
    select! { Token::String(value) => value }.boxed()
}

fn name() -> P<Vec<String>> {
    ident().separated_by(sym('.')).at_least(1).boxed()
}

fn if_not_exists() -> P<bool> {
    kw("IF")
        .then_ignore(kw("NOT"))
        .then_ignore(kw("EXISTS"))
        .or_not()
        .map(|flag| flag.is_some())
        .boxed()
}

fn if_exists() -> P<bool> {
    kw("IF")
        .then_ignore(kw("EXISTS"))
        .or_not()
        .map(|flag| flag.is_some())
        .boxed()
}

fn props() -> P<BTreeMap<String, String>> {
    string()
        .then_ignore(sym('='))
        .then(string())
        .separated_by(sym(','))
        .at_least(1)
        .delimited_by(sym('('), sym(')'))
        .try_map(|pairs, span| {
            let mut values = BTreeMap::new();
            for (key, value) in pairs {
                if values.insert(key.clone(), value).is_some() {
                    return Err(SqlError::custom(span, format!("duplicate property {key}")));
                }
            }
            Ok(values)
        })
        .boxed()
}

fn column(ty: P<DataType>, nested: bool) -> P<Column> {
    let separator = if nested {
        sym(':').or_not().ignored().boxed()
    } else {
        empty().boxed()
    };
    let not_null = kw("NOT").then_ignore(kw("NULL")).or_not().map(|flag| flag.is_none());
    ident()
        .then_ignore(separator)
        .then(ty)
        .then(not_null)
        .then(kw("COMMENT").ignore_then(string()).or_not())
        .map(|(((name, data_type), nullable), comment)| Column {
            name,
            data_type,
            nullable,
            comment,
        })
        .boxed()
}

fn data_type() -> P<DataType> {
    recursive(|ty| {
        let number = select! { Token::Number(value) => value }.try_map(|value, span| {
            value
                .parse::<u8>()
                .map_err(|_| SqlError::custom(span, "number exceeds 255"))
        });
        let decimal = kw("DECIMAL")
            .or(kw("DEC"))
            .or(kw("NUMERIC"))
            .ignore_then(
                number
                    .then_ignore(sym(','))
                    .then(number)
                    .delimited_by(sym('('), sym(')')),
            )
            .try_map(|(precision, scale), span| {
                if precision == 0 || precision > 38 || scale > precision {
                    Err(SqlError::custom(
                        span,
                        "DECIMAL requires 1 <= precision <= 38 and 0 <= scale <= precision",
                    ))
                } else {
                    Ok(DataType::Decimal(precision, scale))
                }
            });
        let structure = kw("STRUCT")
            .ignore_then(
                column(ty.clone().boxed(), true)
                    .separated_by(sym(','))
                    .at_least(1)
                    .delimited_by(sym('<'), sym('>')),
            )
            .map(DataType::Struct);
        let array = kw("ARRAY")
            .ignore_then(ty.clone().delimited_by(sym('<'), sym('>')))
            .map(|element| DataType::Array(Box::new(element)));
        let map = kw("MAP")
            .ignore_then(
                ty.clone()
                    .then_ignore(sym(','))
                    .then(ty)
                    .delimited_by(sym('<'), sym('>')),
            )
            .map(|(key, value)| DataType::Map(Box::new(key), Box::new(value)));
        choice((
            kw("BOOLEAN").or(kw("BOOL")).to(DataType::Boolean),
            kw("TINYINT").or(kw("BYTE")).to(DataType::TinyInt),
            kw("SMALLINT").or(kw("SHORT")).to(DataType::SmallInt),
            kw("INT").or(kw("INTEGER")).to(DataType::Int),
            kw("BIGINT").or(kw("LONG")).to(DataType::BigInt),
            kw("FLOAT").or(kw("REAL")).to(DataType::Float),
            kw("DOUBLE").to(DataType::Double),
            kw("STRING").to(DataType::String),
            kw("BINARY").to(DataType::Binary),
            kw("DATE").to(DataType::Date),
            kw("TIMESTAMP").to(DataType::Timestamp),
            kw("TIMESTAMP_NTZ").to(DataType::TimestampNtz),
            decimal,
            structure,
            array,
            map,
        ))
    })
    .boxed()
}

#[derive(Clone)]
enum Clause {
    Provider,
    Partition(Vec<String>),
    Location(String),
    Comment(String),
    Properties(BTreeMap<String, String>),
}

fn statement() -> P<Statement> {
    let columns = column(data_type(), false)
        .separated_by(sym(','))
        .at_least(1)
        .delimited_by(sym('('), sym(')'))
        .boxed();
    let clause = choice((
        kw("USING").then_ignore(kw("DELTA")).to(Clause::Provider),
        kw("PARTITIONED")
            .then_ignore(kw("BY"))
            .ignore_then(
                ident()
                    .separated_by(sym(','))
                    .at_least(1)
                    .delimited_by(sym('('), sym(')')),
            )
            .map(Clause::Partition),
        kw("LOCATION").ignore_then(string()).map(Clause::Location),
        kw("COMMENT").ignore_then(string()).map(Clause::Comment),
        kw("TBLPROPERTIES").ignore_then(props()).map(Clause::Properties),
    ));
    let create = kw("CREATE")
        .then_ignore(kw("TABLE"))
        .ignore_then(if_not_exists())
        .then(name())
        .then(columns.clone())
        .then(clause.repeated().at_least(1))
        .try_map(|(((if_not_exists, name), columns), clauses), span| {
            let mut metadata = TableMetadata::default();
            let mut seen = BTreeSet::new();
            for clause in clauses {
                let kind = match &clause {
                    Clause::Provider => "USING",
                    Clause::Partition(_) => "PARTITIONED BY",
                    Clause::Location(_) => "LOCATION",
                    Clause::Comment(_) => "COMMENT",
                    Clause::Properties(_) => "TBLPROPERTIES",
                };
                if !seen.insert(kind) {
                    return Err(SqlError::custom(span, format!("duplicate {kind} clause")));
                }
                match clause {
                    Clause::Provider => {}
                    Clause::Partition(value) => metadata.partition_columns = value,
                    Clause::Location(value) => metadata.location = Some(value),
                    Clause::Comment(value) => metadata.comment = Some(value),
                    Clause::Properties(value) => metadata.properties = value,
                }
            }
            if !seen.contains("USING") {
                return Err(SqlError::custom(span, "CREATE TABLE requires USING DELTA"));
            }
            Ok(Statement::CreateTable(CreateTable {
                name,
                if_not_exists,
                columns,
                metadata,
            }))
        });
    let add = kw("ADD")
        .ignore_then(choice((
            kw("COLUMNS").ignore_then(columns),
            kw("COLUMN")
                .ignore_then(column(data_type(), false))
                .map(|column| vec![column]),
        )))
        .map(|columns| (columns,));
    let alter = kw("ALTER")
        .then_ignore(kw("TABLE"))
        .ignore_then(name())
        .then(choice((
            add.map(|(columns,)| Alter::Add(columns)),
            kw("SET")
                .then_ignore(kw("TBLPROPERTIES"))
                .ignore_then(props())
                .map(Alter::Set),
            kw("UNSET")
                .then_ignore(kw("TBLPROPERTIES"))
                .ignore_then(if_exists())
                .then(
                    string()
                        .separated_by(sym(','))
                        .at_least(1)
                        .delimited_by(sym('('), sym(')')),
                )
                .map(|(flag, keys)| Alter::Unset(flag, keys)),
        )))
        .map(|(name, alter)| match alter {
            Alter::Add(columns) => Statement::AddTableColumns { name, columns },
            Alter::Set(properties) => Statement::SetTableProperties { name, properties },
            Alter::Unset(if_exists, keys) => Statement::UnsetTableProperties { name, if_exists, keys },
        });
    let namespace_clause = choice((
        kw("COMMENT").ignore_then(string()).map(Clause::Comment),
        kw("LOCATION").ignore_then(string()).map(Clause::Location),
        kw("WITH")
            .then_ignore(kw("DBPROPERTIES"))
            .ignore_then(props())
            .map(Clause::Properties),
    ));
    let namespace = kw("CREATE")
        .then_ignore(kw("DATABASE").or(kw("SCHEMA")))
        .ignore_then(if_not_exists())
        .then(name())
        .then(namespace_clause.repeated())
        .try_map(|((if_not_exists, name), clauses), span| {
            let mut namespace = CreateNamespace {
                name,
                if_not_exists,
                comment: None,
                location: None,
                properties: BTreeMap::new(),
            };
            let mut seen = BTreeSet::new();
            for clause in clauses {
                let kind = match clause {
                    Clause::Comment(_) => "COMMENT",
                    Clause::Location(_) => "LOCATION",
                    _ => "DBPROPERTIES",
                };
                if !seen.insert(kind) {
                    return Err(SqlError::custom(span, format!("duplicate {kind} clause")));
                }
                match clause {
                    Clause::Comment(value) => namespace.comment = Some(value),
                    Clause::Location(value) => namespace.location = Some(value),
                    Clause::Properties(value) => namespace.properties = value,
                    _ => unreachable!(),
                }
            }
            Ok(Statement::CreateNamespace(namespace))
        });
    let drop = kw("DROP")
        .then_ignore(kw("TABLE"))
        .ignore_then(if_exists())
        .then(name())
        .map(|(if_exists, name)| Statement::DropTable { name, if_exists });
    let show_tables = kw("SHOW")
        .then_ignore(kw("TABLES"))
        .ignore_then(kw("IN").or(kw("FROM")).ignore_then(name()).or_not())
        .then(kw("LIKE").ignore_then(string()).or_not())
        .map(|(namespace, pattern)| Statement::ShowTables { namespace, pattern });
    let show_props = kw("SHOW")
        .then_ignore(kw("TBLPROPERTIES"))
        .ignore_then(name())
        .then(string().delimited_by(sym('('), sym(')')).or_not())
        .map(|(name, key)| Statement::ShowTableProperties { name, key });
    let describe = kw("DESCRIBE")
        .or(kw("DESC"))
        .then_ignore(kw("TABLE").or_not())
        .ignore_then(name())
        .map(|name| Statement::DescribeTable { name });
    choice((create, alter, namespace, drop, show_tables, show_props, describe))
        .then_ignore(sym(';').or_not())
        .then_ignore(end())
        .boxed()
}

#[derive(Clone)]
enum Alter {
    Add(Vec<Column>),
    Set(BTreeMap<String, String>),
    Unset(bool, Vec<String>),
}

fn lexer() -> impl Parser<char, Vec<(Token, Range<usize>)>, Error = Simple<char>> {
    let quoted_ident = just("``")
        .to('`')
        .or(filter(|c: &char| *c != '`'))
        .repeated()
        .collect::<String>()
        .delimited_by(just('`'), just('`'))
        .map(Token::Ident);
    let escaped = just('\\').ignore_then(choice((
        just('n').to('\n'),
        just('r').to('\r'),
        just('t').to('\t'),
        just('\\').to('\\'),
        just('\'').to('\''),
        just('"').to('"'),
    )));
    let literal = just("''")
        .to('\'')
        .or(escaped)
        .or(filter(|c: &char| *c != '\'' && *c != '\\'))
        .repeated()
        .collect::<String>()
        .delimited_by(just('\''), just('\''))
        .map(Token::String);
    let word = text::ident().map(Token::Word);
    let number = text::int(10).map(Token::Number);
    let symbol = one_of("(),.<>;=:").map(Token::Symbol);
    let block = just("/*")
        .ignore_then(just("*/").not().ignore_then(any()).repeated())
        .then_ignore(just("*/"))
        .ignored();
    let line = just("--")
        .ignore_then(filter(|c: &char| *c != '\n').repeated())
        .ignored();
    let skip = choice((text::whitespace().at_least(1).ignored(), line, block)).repeated();
    choice((quoted_ident, literal, number, word, symbol))
        .map_with_span(|token, span| (token, span))
        .padded_by(skip)
        .repeated()
        .then_ignore(end())
}

fn validate_columns(columns: &[Column]) -> Result<(), String> {
    let mut names = BTreeSet::new();
    for column in columns {
        if !names.insert(&column.name) {
            return Err(format!("duplicate column {}", column.name));
        }
        validate_type(&column.data_type)?;
    }
    Ok(())
}

fn validate_type(ty: &DataType) -> Result<(), String> {
    match ty {
        DataType::Struct(columns) => validate_columns(columns),
        DataType::Array(element) => validate_type(element),
        DataType::Map(key, value) => {
            if !matches!(
                key.as_ref(),
                DataType::Boolean
                    | DataType::TinyInt
                    | DataType::SmallInt
                    | DataType::Int
                    | DataType::BigInt
                    | DataType::String
            ) {
                return Err("MAP keys must be portable integral, boolean, or string types".to_owned());
            }
            validate_type(value)
        }
        _ => Ok(()),
    }
}

fn validate(statement: &Statement) -> Result<(), String> {
    match statement {
        Statement::CreateTable(table) => {
            validate_columns(&table.columns)?;
            let mut seen = BTreeSet::new();
            for name in &table.metadata.partition_columns {
                if !seen.insert(name) {
                    return Err(format!("duplicate partition column {name}"));
                }
                let field = table
                    .columns
                    .iter()
                    .find(|field| &field.name == name)
                    .ok_or_else(|| format!("unknown partition column {name}"))?;
                if matches!(
                    field.data_type,
                    DataType::Struct(_) | DataType::Array(_) | DataType::Map(_, _)
                ) {
                    return Err(format!("partition column {name} must have a primitive type"));
                }
            }
            Ok(())
        }
        Statement::AddTableColumns { columns, .. } => {
            validate_columns(columns)?;
            if columns.iter().any(|field| !field.nullable) {
                return Err("ADD COLUMNS requires nullable additions".to_owned());
            }
            Ok(())
        }
        Statement::UnsetTableProperties { keys, .. } => {
            if keys.iter().collect::<BTreeSet<_>>().len() != keys.len() {
                return Err("duplicate property key".to_owned());
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Parses exactly one statement from the explicit Spark catalog DDL subset.
///
/// # Errors
///
/// Returns byte-spanned diagnostics for invalid syntax, unsupported clauses,
/// unsupported types, duplicate declarations, or non-nullable additions.
pub fn parse(sql: &str) -> Result<Statement, Vec<ParseError>> {
    if sql.is_empty() || sql.len() > 65_536 {
        return Err(vec![ParseError {
            span: 0..sql.len(),
            message: "DDL must contain 1 to 65536 bytes".to_owned(),
        }]);
    }
    let offsets = sql
        .char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(sql.len()))
        .collect::<Vec<_>>();
    let byte_span =
        |span: Range<usize>| offsets[span.start.min(offsets.len() - 1)]..offsets[span.end.min(offsets.len() - 1)];
    let tokens = lexer().parse(sql).map_err(|errors| {
        errors
            .into_iter()
            .map(|error| ParseError {
                span: byte_span(error.span()),
                message: error.to_string(),
            })
            .collect::<Vec<_>>()
    })?;
    let tokens = tokens
        .into_iter()
        .map(|(token, span)| (token, byte_span(span)))
        .collect::<Vec<_>>();
    let mut depth = 0_u32;
    for (token, span) in &tokens {
        match token {
            Token::Symbol('(' | '<') => {
                depth += 1;
                if depth > 64 {
                    return Err(vec![ParseError {
                        span: span.clone(),
                        message: "DDL nesting exceeds 64 levels".to_owned(),
                    }]);
                }
            }
            Token::Symbol(')' | '>') => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    let statement = statement()
        .parse(Stream::from_iter(sql.len()..sql.len(), tokens.into_iter()))
        .map_err(|errors| {
            errors
                .into_iter()
                .map(|error| ParseError {
                    span: error.span(),
                    message: error.to_string(),
                })
                .collect::<Vec<_>>()
        })?;
    validate(&statement).map_err(|message| {
        vec![ParseError {
            span: 0..sql.len(),
            message,
        }]
    })?;
    Ok(statement)
}
