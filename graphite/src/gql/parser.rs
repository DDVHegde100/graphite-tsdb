//! Hand-written recursive descent parser for GQL.
//! Grammar:
//!   EXPLAIN? SELECT {columns|*} FROM {symbol}
//!   WHERE timestamp BETWEEN {t1} AND {t2} [AND price > {x}]
//!   [LIMIT n] [GROUP BY {interval} AGGREGATE {fn}]

use super::ast::*;
use graphite_core::Column;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum ParseError {
    #[error("Unexpected token at position {pos}: expected {expected}, got '{got}'")]
    UnexpectedToken {
        pos: usize,
        expected: String,
        got: String,
    },
    #[error("Parse error at position {pos}: {message}")]
    Message { pos: usize, message: String },
    #[error("Unexpected end of input at position {pos}, expected {expected}")]
    UnexpectedEof { pos: usize, expected: String },
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Explain,
    Select,
    From,
    Where,
    Timestamp,
    Between,
    And,
    Price,
    Greater,
    Less,
    GreaterEq,
    LessEq,
    Equal,
    Limit,
    GroupBy,
    Aggregate,
    Star,
    Identifier(String),
    Number(f64),
    Integer(i64),
    Colon,
    Sec1,
    Min1,
    Hour1,
    Ohlcv,
    Sum,
    Count,
    Vwap,
}

struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.input.get(self.pos).copied();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn read_identifier(&mut self) -> String {
        let mut s = String::new();
        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                s.push(self.advance().unwrap());
            } else {
                break;
            }
        }
        s
    }

    fn read_number(&mut self) -> Result<f64, ParseError> {
        let mut s = String::new();
        let mut has_dot = false;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                s.push(self.advance().unwrap());
            } else if c == '.' && !has_dot {
                has_dot = true;
                s.push(self.advance().unwrap());
            } else if c == '-' && s.is_empty() {
                s.push(self.advance().unwrap());
            } else {
                break;
            }
        }
        s.parse::<f64>()
            .map_err(|_| ParseError::Message {
                pos: self.pos,
                message: format!("invalid number: {s}"),
            })
    }

    fn keyword_token(word: &str) -> Option<Token> {
        match word.to_uppercase().as_str() {
            "EXPLAIN" => Some(Token::Explain),
            "SELECT" => Some(Token::Select),
            "FROM" => Some(Token::From),
            "WHERE" => Some(Token::Where),
            "TIMESTAMP" => Some(Token::Timestamp),
            "BETWEEN" => Some(Token::Between),
            "AND" => Some(Token::And),
            "PRICE" => Some(Token::Price),
            "LIMIT" => Some(Token::Limit),
            "GROUP" => None, // handled specially
            "BY" => None,
            "AGGREGATE" => Some(Token::Aggregate),
            "OHLCV" => Some(Token::Ohlcv),
            "SUM" => Some(Token::Sum),
            "COUNT" => Some(Token::Count),
            "VWAP" => Some(Token::Vwap),
            "1S" => Some(Token::Sec1),
            "1M" => Some(Token::Min1),
            "1H" => Some(Token::Hour1),
            _ => None,
        }
    }

    fn next_token(&mut self) -> Result<Option<Token>, ParseError> {
        self.skip_whitespace();

        let c = match self.peek() {
            Some(c) => c,
            None => return Ok(None),
        };

        match c {
            '*' => {
                self.advance();
                Ok(Some(Token::Star))
            }
            '>' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    Ok(Some(Token::GreaterEq))
                } else {
                    Ok(Some(Token::Greater))
                }
            }
            '<' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    Ok(Some(Token::LessEq))
                } else {
                    Ok(Some(Token::Less))
                }
            }
            '=' => {
                self.advance();
                Ok(Some(Token::Equal))
            }
            ':' => {
                self.advance();
                Ok(Some(Token::Colon))
            }
            c if c.is_alphabetic() || c == '_' => {
                let word = self.read_identifier();
                if word.to_uppercase() == "GROUP" {
                    self.skip_whitespace();
                    let by = self.read_identifier();
                    if by.to_uppercase() != "BY" {
                        return Err(ParseError::Message {
                            pos: self.pos,
                            message: "expected BY after GROUP".into(),
                        });
                    }
                    return Ok(Some(Token::GroupBy));
                }
                if let Some(token) = Self::keyword_token(&word) {
                    Ok(Some(token))
                } else {
                    Ok(Some(Token::Identifier(word)))
                }
            }
            c if c.is_ascii_digit() || c == '-' => {
                let num = self.read_number()?;
                if num.fract() == 0.0 && num.abs() < 1e15 {
                    Ok(Some(Token::Integer(num as i64)))
                } else {
                    Ok(Some(Token::Number(num)))
                }
            }
            _ => Err(ParseError::UnexpectedToken {
                pos: self.pos,
                expected: "valid token".into(),
                got: c.to_string(),
            }),
        }
    }

    fn tokenize(&mut self) -> Result<Vec<Token>, ParseError> {
        let mut tokens = Vec::new();
        while let Some(token) = self.next_token()? {
            tokens.push(token);
        }
        Ok(tokens)
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        match self.advance() {
            Some(t) if t == expected => Ok(()),
            Some(got) => Err(ParseError::UnexpectedToken {
                pos: self.pos,
                expected: format!("{expected:?}"),
                got: format!("{got:?}"),
            }),
            None => Err(ParseError::UnexpectedEof {
                pos: self.pos,
                expected: format!("{expected:?}"),
            }),
        }
    }

    fn parse_column_name(name: &str) -> Option<Column> {
        match name.to_lowercase().as_str() {
            "timestamp" => Some(Column::Timestamp),
            "symbol" => Some(Column::Symbol),
            "open" => Some(Column::Open),
            "high" => Some(Column::High),
            "low" => Some(Column::Low),
            "close" => Some(Column::Close),
            "volume" => Some(Column::Volume),
            _ => None,
        }
    }

    fn parse(&mut self) -> Result<Query, ParseError> {
        let explain = if self.peek() == Some(&Token::Explain) {
            self.advance();
            true
        } else {
            false
        };

        self.expect(Token::Select)?;

        let columns = if self.peek() == Some(&Token::Star) {
            self.advance();
            SelectColumns::All
        } else {
            let mut cols = Vec::new();
            loop {
                match self.advance() {
                    Some(Token::Identifier(name)) => {
                        if let Some(col) = Self::parse_column_name(&name) {
                            cols.push(col);
                        } else {
                            return Err(ParseError::Message {
                                pos: self.pos,
                                message: format!("unknown column: {name}"),
                            });
                        }
                    }
                    other => {
                        return Err(ParseError::UnexpectedToken {
                            pos: self.pos,
                            expected: "column name".into(),
                            got: format!("{other:?}"),
                        });
                    }
                }
                if self.peek() == Some(&Token::From) {
                    break;
                }
                // comma-separated columns — skip comma if present
                // (lexer doesn't produce commas, so columns are space-separated)
            }
            SelectColumns::Columns(cols)
        };

        self.expect(Token::From)?;
        let symbol = match self.advance() {
            Some(Token::Identifier(s)) => s,
            other => {
                return Err(ParseError::UnexpectedToken {
                    pos: self.pos,
                    expected: "symbol name".into(),
                    got: format!("{other:?}"),
                });
            }
        };

        self.expect(Token::Where)?;
        self.expect(Token::Timestamp)?;
        self.expect(Token::Between)?;

        let t1 = match self.advance() {
            Some(Token::Integer(n)) => n,
            other => {
                return Err(ParseError::UnexpectedToken {
                    pos: self.pos,
                    expected: "timestamp integer".into(),
                    got: format!("{other:?}"),
                });
            }
        };

        self.expect(Token::And)?;

        let t2 = match self.advance() {
            Some(Token::Integer(n)) => n,
            other => {
                return Err(ParseError::UnexpectedToken {
                    pos: self.pos,
                    expected: "timestamp integer".into(),
                    got: format!("{other:?}"),
                });
            }
        };

        let price_predicate = if self.peek() == Some(&Token::And) {
            self.advance();
            self.expect(Token::Price)?;
            let pred = match self.advance() {
                Some(Token::Greater) => {
                    let val = self.parse_number()?;
                    PricePredicate::Greater(val)
                }
                Some(Token::Less) => {
                    let val = self.parse_number()?;
                    PricePredicate::Less(val)
                }
                Some(Token::GreaterEq) => {
                    let val = self.parse_number()?;
                    PricePredicate::GreaterEq(val)
                }
                Some(Token::LessEq) => {
                    let val = self.parse_number()?;
                    PricePredicate::LessEq(val)
                }
                Some(Token::Equal) => {
                    let val = self.parse_number()?;
                    PricePredicate::Equal(val)
                }
                other => {
                    return Err(ParseError::UnexpectedToken {
                        pos: self.pos,
                        expected: "price comparator".into(),
                        got: format!("{other:?}"),
                    });
                }
            };
            Some(pred)
        } else {
            None
        };

        let limit = if self.peek() == Some(&Token::Limit) {
            self.advance();
            match self.advance() {
                Some(Token::Integer(n)) => Some(n as u64),
                other => {
                    return Err(ParseError::UnexpectedToken {
                        pos: self.pos,
                        expected: "limit integer".into(),
                        got: format!("{other:?}"),
                    });
                }
            }
        } else {
            None
        };

        let group_by = if self.peek() == Some(&Token::GroupBy) {
            self.advance();
            self.expect(Token::Colon)?;
            let interval = match self.advance() {
                Some(Token::Sec1) => Interval::Sec1,
                Some(Token::Min1) => Interval::Min1,
                Some(Token::Hour1) => Interval::Hour1,
                Some(Token::Integer(1)) => match self.advance() {
                    Some(Token::Identifier(suffix)) => match suffix.to_lowercase().as_str() {
                        "s" => Interval::Sec1,
                        "m" => Interval::Min1,
                        "h" => Interval::Hour1,
                        other => {
                            return Err(ParseError::Message {
                                pos: self.pos,
                                message: format!("unknown interval suffix: {other}"),
                            });
                        }
                    },
                    other => {
                        return Err(ParseError::UnexpectedToken {
                            pos: self.pos,
                            expected: "interval suffix (s|m|h)".into(),
                            got: format!("{other:?}"),
                        });
                    }
                },
                other => {
                    return Err(ParseError::UnexpectedToken {
                        pos: self.pos,
                        expected: "interval (1s|1m|1h)".into(),
                        got: format!("{other:?}"),
                    });
                }
            };
            self.expect(Token::Aggregate)?;
            let aggregate = match self.advance() {
                Some(Token::Ohlcv) => AggregateFn::Ohlcv,
                Some(Token::Sum) => AggregateFn::Sum,
                Some(Token::Count) => AggregateFn::Count,
                Some(Token::Vwap) => AggregateFn::Vwap,
                other => {
                    return Err(ParseError::UnexpectedToken {
                        pos: self.pos,
                        expected: "aggregate function".into(),
                        got: format!("{other:?}"),
                    });
                }
            };
            Some(GroupByClause {
                interval,
                aggregate,
            })
        } else {
            None
        };

        Ok(Query {
            explain,
            columns,
            symbol,
            where_clause: WhereClause {
                t1,
                t2,
                price_predicate,
            },
            limit,
            group_by,
        })
    }

    fn parse_number(&mut self) -> Result<f64, ParseError> {
        match self.advance() {
            Some(Token::Number(n)) => Ok(n),
            Some(Token::Integer(n)) => Ok(n as f64),
            other => Err(ParseError::UnexpectedToken {
                pos: self.pos,
                expected: "number".into(),
                got: format!("{other:?}"),
            }),
        }
    }
}

pub fn parse(input: &str) -> Result<Query, ParseError> {
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_select() {
        let q = parse("SELECT * FROM AAPL WHERE timestamp BETWEEN 1000 AND 2000").unwrap();
        assert_eq!(q.symbol, "AAPL");
        assert_eq!(q.where_clause.t1, 1000);
        assert_eq!(q.where_clause.t2, 2000);
        assert!(!q.explain);
    }

    #[test]
    fn parse_with_price_predicate() {
        let q =
            parse("SELECT close FROM AAPL WHERE timestamp BETWEEN 0 AND 999999 AND price > 150.0")
                .unwrap();
        assert_eq!(
            q.where_clause.price_predicate,
            Some(PricePredicate::Greater(150.0))
        );
    }

    #[test]
    fn parse_group_by() {
        let q = parse(
            "SELECT * FROM AAPL WHERE timestamp BETWEEN 0 AND 999999 GROUP BY :1m AGGREGATE OHLCV",
        )
        .unwrap();
        assert!(q.group_by.is_some());
        assert_eq!(q.group_by.unwrap().interval, Interval::Min1);
    }

    #[test]
    fn parse_explain() {
        let q = parse("EXPLAIN SELECT * FROM AAPL WHERE timestamp BETWEEN 0 AND 1000").unwrap();
        assert!(q.explain);
    }
}
