package blackboard

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// tokenKind is the lexical class of one precondition token.
type tokenKind int

const (
	tokenEOF tokenKind = iota
	tokenKey
	tokenPattern
	tokenString
	tokenDuration
	tokenEq
	tokenNeq
	tokenColon
	tokenLParen
	tokenRParen
	tokenAnd
	tokenOr
	tokenNot
	tokenIs
	tokenSet
	tokenChanged
	tokenOlderThan
)

// String names a token kind as parse errors spell it.
func (k tokenKind) String() string {
	switch k {
	case tokenEOF:
		return "EOF"
	case tokenKey:
		return "KEY"
	case tokenPattern:
		return "PATTERN"
	case tokenString:
		return "STRING"
	case tokenDuration:
		return "DURATION"
	case tokenEq:
		return "=="
	case tokenNeq:
		return "!="
	case tokenColon:
		return ":"
	case tokenLParen:
		return "("
	case tokenRParen:
		return ")"
	case tokenAnd:
		return "AND"
	case tokenOr:
		return "OR"
	case tokenNot:
		return "NOT"
	case tokenIs:
		return "IS"
	case tokenSet:
		return "SET"
	case tokenChanged:
		return "CHANGED"
	case tokenOlderThan:
		return "OLDER_THAN"
	default:
		return "UNKNOWN"
	}
}

// keywords are the bare words the grammar reserves; any other word is a key.
var keywords = map[string]tokenKind{
	"AND":        tokenAnd,
	"OR":         tokenOr,
	"NOT":        tokenNot,
	"IS":         tokenIs,
	"SET":        tokenSet,
	"CHANGED":    tokenChanged,
	"OLDER_THAN": tokenOlderThan,
}

// token is one lexed token: its kind, its source text, and where it started.
type token struct {
	kind tokenKind
	text string
	pos  int
}

// tokenize splits an expression into tokens, reporting the first lexical error it meets.
func tokenize(expression string) ([]token, error) {
	var tokens []token
	for i := 0; i < len(expression); {
		char, width := utf8.DecodeRuneInString(expression[i:])
		switch {
		case unicode.IsSpace(char):
			i += width
		case char == '(':
			tokens = append(tokens, token{kind: tokenLParen, text: "(", pos: i})
			i++
		case char == ')':
			tokens = append(tokens, token{kind: tokenRParen, text: ")", pos: i})
			i++
		case strings.HasPrefix(expression[i:], "=="):
			tokens = append(tokens, token{kind: tokenEq, text: "==", pos: i})
			i += 2
		case strings.HasPrefix(expression[i:], "!="):
			tokens = append(tokens, token{kind: tokenNeq, text: "!=", pos: i})
			i += 2
		case char == ':':
			tokens = append(tokens, token{kind: tokenColon, text: ":", pos: i})
			i++
		case char == '"' || char == '\'':
			literal, next, err := scanString(expression, i)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, literal)
			i = next
		case isDigit(expression[i]):
			duration, next, err := scanDuration(expression, i)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, duration)
			i = next
		default:
			end := wordEnd(expression, i)
			if end == i {
				return nil, fmt.Errorf("%w: unexpected character %q at position %d", ErrPreconditionParse, char, i)
			}
			word := expression[i:end]
			kind, isKeyword := keywords[word]
			switch {
			case isKeyword:
			case strings.ContainsAny(word, "*>"):
				if !validKeyPattern(word) {
					return nil, fmt.Errorf("%w: invalid key pattern %q at position %d", ErrPreconditionParse, word, i)
				}
				kind = tokenPattern
			default:
				kind = tokenKey
			}
			tokens = append(tokens, token{kind: kind, text: word, pos: i})
			i = end
		}
	}
	return append(tokens, token{kind: tokenEOF, text: "", pos: len(expression)}), nil
}

// scanString reads a single- or double-quoted literal, which runs to the next matching quote.
func scanString(expression string, start int) (token, int, error) {
	quote := expression[start]
	for i := start + 1; i < len(expression); i++ {
		if expression[i] == quote {
			return token{kind: tokenString, text: expression[start+1 : i], pos: start}, i + 1, nil
		}
	}
	return token{}, 0, fmt.Errorf("%w: unterminated string at position %d", ErrPreconditionParse, start)
}

// scanDuration reads an integer amount followed by its unit, the only place digits may start a token.
func scanDuration(expression string, start int) (token, int, error) {
	end := start
	for end < len(expression) && isDigit(expression[end]) {
		end++
	}
	if end >= len(expression) || !strings.ContainsRune("smh", rune(expression[end])) {
		return token{}, 0, fmt.Errorf("%w: invalid duration at position %d", ErrPreconditionParse, start)
	}
	end++
	return token{kind: tokenDuration, text: expression[start:end], pos: start}, end, nil
}

// wordEnd returns the end of the [a-zA-Z0-9._*>]+ run starting at i, or i when there is none; the
// wildcard bytes make the run a key pattern (§4 rule 10).
func wordEnd(expression string, i int) int {
	end := i
	for end < len(expression) && (isWordByte(expression[end]) || expression[end] == '*' || expression[end] == '>') {
		end++
	}
	return end
}

// validKeyPattern reports whether a word is a read pattern: tokens of [a-zA-Z0-9_]+ or "*", and ">" last only (§4 rule 10).
func validKeyPattern(word string) bool {
	tokens := strings.Split(word, ".")
	for i, part := range tokens {
		switch {
		case part == "*":
		case part == ">":
			if i != len(tokens)-1 {
				return false
			}
		case part == "" || strings.ContainsAny(part, "*>"):
			return false
		}
	}
	return true
}

// isDigit reports whether a byte is an ASCII digit.
func isDigit(char byte) bool { return char >= '0' && char <= '9' }

// isWordByte reports whether a byte may appear in a key or field name.
func isWordByte(char byte) bool {
	switch {
	case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', isDigit(char):
		return true
	default:
		return char == '.' || char == '_'
	}
}

// durationSeconds converts a duration token's text to whole seconds.
func durationSeconds(text string, pos int) (int64, error) {
	invalid := fmt.Errorf("%w: invalid duration at position %d", ErrPreconditionParse, pos)
	amount, err := strconv.ParseInt(text[:len(text)-1], 10, 64)
	if err != nil {
		return 0, invalid
	}
	multiplier := int64(1)
	switch text[len(text)-1] {
	case 'm':
		multiplier = 60
	case 'h':
		multiplier = 3600
	}
	if amount > math.MaxInt64/multiplier {
		return 0, invalid
	}
	return amount * multiplier, nil
}

// parser turns a token stream into an expression tree, by the §4 grammar.
type parser struct {
	tokens []token
	pos    int
}

// current returns the token the parser is looking at.
func (p *parser) current() token { return p.tokens[p.pos] }

// advance consumes and returns the current token.
func (p *parser) advance() token {
	consumed := p.tokens[p.pos]
	p.pos++
	return consumed
}

// expect consumes the current token when it is of the wanted kind, and errors otherwise.
func (p *parser) expect(kind tokenKind) (token, error) {
	current := p.current()
	if current.kind != kind {
		return token{}, fmt.Errorf("%w: expected %s but got %q at position %d",
			ErrPreconditionParse, kind, current.text, current.pos)
	}
	return p.advance(), nil
}

// parse reads the whole token stream as one expression.
func (p *parser) parse() (node, error) {
	root, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if current := p.current(); current.kind != tokenEOF {
		return nil, fmt.Errorf("%w: unexpected token %q at position %d", ErrPreconditionParse, current.text, current.pos)
	}
	return root, nil
}

// parseOr reads and_expr ("OR" and_expr)*, the loosest-binding level.
func (p *parser) parseOr() (node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.current().kind == tokenOr {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = orNode{left: left, right: right}
	}
	return left, nil
}

// parseAnd reads not_expr ("AND" not_expr)*.
func (p *parser) parseAnd() (node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.current().kind == tokenAnd {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = andNode{left: left, right: right}
	}
	return left, nil
}

// parseNot reads "NOT" not_expr | atom.
func (p *parser) parseNot() (node, error) {
	if p.current().kind != tokenNot {
		return p.parseAtom()
	}
	p.advance()
	operand, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	return notNode{operand: operand}, nil
}

// parseAtom reads a parenthesized expression or a reference followed by its operator.
func (p *parser) parseAtom() (node, error) {
	current := p.current()
	if current.kind == tokenLParen {
		p.advance()
		group, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return group, nil
	}
	if current.kind == tokenPattern {
		p.advance()
		return p.parsePatternOperator(keyPattern(current.text))
	}
	if current.kind != tokenKey {
		return nil, fmt.Errorf("%w: expected key or '(' but got %q at position %d",
			ErrPreconditionParse, current.text, current.pos)
	}

	reference := ref{key: p.advance().text}
	if p.current().kind == tokenColon {
		p.advance()
		field := p.current()
		if field.kind != tokenKey {
			return nil, fmt.Errorf("%w: expected field name after ':' but got %q at position %d",
				ErrPreconditionParse, field.text, field.pos)
		}
		reference.field = p.advance().text
	}
	return p.parseOperator(reference)
}

// parseOperator reads the operator that follows a reference, and the operand it takes.
func (p *parser) parseOperator(reference ref) (node, error) {
	operator := p.current()
	switch operator.kind {
	case tokenEq:
		p.advance()
		literal, err := p.expect(tokenString)
		if err != nil {
			return nil, err
		}
		return eqNode{ref: reference, literal: literal.text}, nil
	case tokenNeq:
		p.advance()
		literal, err := p.expect(tokenString)
		if err != nil {
			return nil, err
		}
		return neqNode{ref: reference, literal: literal.text}, nil
	case tokenIs:
		p.advance()
		return p.parseIs(reference)
	case tokenChanged:
		p.advance()
		return changedNode{ref: reference}, nil
	case tokenOlderThan:
		return p.parseOlderThan(reference)
	default:
		return nil, fmt.Errorf("%w: expected operator after key but got %q at position %d",
			ErrPreconditionParse, operator.text, operator.pos)
	}
}

// parsePatternOperator reads the operator after a key pattern: IS SET, IS NOT SET or CHANGED only (§4 rule 10).
func (p *parser) parsePatternOperator(pattern keyPattern) (node, error) {
	operator := p.current()
	switch operator.kind {
	case tokenIs:
		p.advance()
		negated, err := p.parseIsTail()
		if err != nil {
			return nil, err
		}
		if negated {
			return patternIsNotSetNode{pattern: pattern}, nil
		}
		return patternIsSetNode{pattern: pattern}, nil
	case tokenChanged:
		p.advance()
		return patternChangedNode{pattern: pattern}, nil
	case tokenColon:
		return nil, fmt.Errorf("%w: a key pattern takes no field accessor at position %d",
			ErrPreconditionParse, operator.pos)
	case tokenEq, tokenNeq, tokenOlderThan:
		return nil, fmt.Errorf("%w: %s does not accept a key pattern at position %d",
			ErrPreconditionParse, operator.kind, operator.pos)
	default:
		return nil, fmt.Errorf("%w: expected IS or CHANGED after key pattern but got %q at position %d",
			ErrPreconditionParse, operator.text, operator.pos)
	}
}

// parseIs reads the SET or NOT SET tail of an IS operator.
func (p *parser) parseIs(reference ref) (node, error) {
	negated, err := p.parseIsTail()
	if err != nil {
		return nil, err
	}
	if negated {
		return isNotSetNode{ref: reference}, nil
	}
	return isSetNode{ref: reference}, nil
}

// parseIsTail reads SET or NOT SET after IS, reporting whether it was NOT SET.
func (p *parser) parseIsTail() (bool, error) {
	next := p.current()
	switch next.kind {
	case tokenSet:
		p.advance()
		return false, nil
	case tokenNot:
		p.advance()
		if _, err := p.expect(tokenSet); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, fmt.Errorf("%w: expected SET or NOT after IS but got %q at position %d",
			ErrPreconditionParse, next.text, next.pos)
	}
}

// parseOlderThan reads a duration operand, which the grammar allows on a bare key only (§4 rule 4).
func (p *parser) parseOlderThan(reference ref) (node, error) {
	operator := p.advance()
	if reference.field != "" {
		return nil, fmt.Errorf("%w: OLDER_THAN does not accept a field accessor at position %d",
			ErrPreconditionParse, operator.pos)
	}
	duration, err := p.expect(tokenDuration)
	if err != nil {
		return nil, err
	}
	seconds, err := durationSeconds(duration.text, duration.pos)
	if err != nil {
		return nil, err
	}
	return olderThanNode{key: reference.key, seconds: seconds}, nil
}
