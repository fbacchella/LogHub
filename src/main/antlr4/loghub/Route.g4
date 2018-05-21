//
// A mix of DSL grammar, Java grammar and Groovy grammar
// Java grammar is taken from https://github.com/antlr/grammars-v4/blob/master/java/Java.g4

grammar Route;

configuration: (pipeline|input|output|sources|property)+ EOF;
pipeline: 'pipeline' '[' Identifier ']' '{' pipenodeList? '}' ( '|' '$' finalpiperef) ?;
input: 'input' '{'  inputObjectlist '}' ('|' '$' piperef)?;
output: 'output' ('$' piperef '|' )? '{' outputObjectlist '}';
inputObjectlist: (object (',' object)*)? ','?;
outputObjectlist: (object (',' object)*)? ','?;
//pipenodeList: (pipenode (('+' forkpiperef)|('|' pipenode))*) | ( (pipenode (('+' forkpiperef)|('|' pipenode))*)? '>' forwardpiperef)?;
pipenodeList: (pipenode (('+' forkpiperef)|('|' pipenode))*) ('>' forwardpiperef)?
              |  ('>' forwardpiperef)
              | ('+' forkpiperef)
;
forkpiperef: '$' Identifier;
forwardpiperef: '$' Identifier;
pipenode
    : test
    | merge
    | mapping
    | drop
    | fire
    | log
    | etl
    | '(' pipenodeList ')'
    | '$' piperef
    | object
    | '{' pipenodeList? '}'
    | path
    ;

object: QualifiedIdentifier beansDescription ; 
beansDescription: ('{' (bean (',' bean)*)? ','? '}')? ;

bean
    :   'if' ':' expression
    |   condition=('success' | 'failure' | 'exception') ':' pipenode
    |   (beanName ':' beanValue)
    ;

beanName
    :'index' | 'seeds' | 'doFire' | 'onFire' | 'timeout' | 'forward' | 'default' | 'merge' | 'inPipeline' | 'path'
    | Identifier
    ;

beanValue: object | literal | array | map | expression;
finalpiperef: piperef;

piperef:  Identifier;

merge
    : 'merge' '{' (mergeArgument (',' mergeArgument)*)? ','? '}'
    ;

mergeArgument
    : type='if' ':' expression
    | type='index' ':' stringLiteral
    | type='seeds' ':' map
    | type='doFire' ':' expression
    | type='onFire' ':' pipenode
    | type='onTimeout' ':' pipenode
    | type='timeout' ':' (integerLiteral | floatingPointLiteral)
    | type='forward' ':' booleanLiteral
    | type='default' ':' beanValue
    | type='inPipeline' ':' stringLiteral
    ;

mapping
    :   'map' eventVariable map
    ;

drop: Drop;
Drop: 'drop';

fire
    : 'fire' '{' ( eventVariable '=' expression ';'? )* eventVariable '=' expression ';'? '}' '>' '$' piperef 
    ;

log
    : 'log' '(' message=StringLiteral ',' level ')'
    ;

level
    : 'FATAL'
    | 'ERROR'
    | 'WARN'
    | 'INFO'
    | 'DEBUG'
    | 'TRACE'
    ;

property: propertyName ':' beanValue;

etl
    : eventVariable op='<' s=eventVariable
    | eventVariable op='-'
    | eventVariable op='=' expression
    | eventVariable op='@' e=expression map
    | op='(' QualifiedIdentifier ')' eventVariable
    ;

path
    : 'path' eventVariable '(' pipenodeList ')'
    ;
 
test: testExpression '?' pipenode (':' pipenode)? ;

testExpression: expression;

expression
    :   sl = stringLiteral ( expressionsList )?
    |   l = literal
    |   ev = eventVariable
    |   qi = QualifiedIdentifier
    |   opu = unaryOperator e2 = expression
    |   'new' newclass = (QualifiedIdentifier | Identifier) '(' expression ')'
    |   e1 = expression opb=binaryOperator e2=expression
    |   e1 = expression opm=matchOperator patternLiteral
    |   '(' e3 = expression ')'
    |   expression '[' arrayIndex=IntegerLiteral ']'
    ;

expressionsList
    : '(' expression ( ','  expression )* ','? ')'
    ;

unaryOperator
    :   '~'
    |   '.~'
    |   '!'
    |   '+'
    |   '-'
    ;

binaryOperator
    :   '**'
    |   '*'
    |   '/'
    |   '%'
    |   '+'
    |   '-'
    |   '<<'
    |   '>>>'
    |   '>>'
    |   '<='
    |   '>='
    |   '>'
    |   '<'
    |   'instanceof'
    |   'in'
    |   '=='
    |   '==='
    |   '!='
    |   '<=>'
    |   '!=='
    |   '.&'
    |   '.^'
    |   '.|'
    |   '&&'
    |   '||'
    ;

matchOperator
    :   '=~'
    |   '==~'
    ;

array
    : '[' (beanValue (',' beanValue)*)? ','? ']'
    | source
    ;

map
    : '{' (literal ':' beanValue ( ',' ? literal ':' beanValue)*)? ','? '}'
    | source
    ;

source
    : '%' Identifier
    ;

eventVariable: '[' (key='@timestamp' | (key='@context' (Identifier ( Identifier)*))? | MetaName | (Identifier ( Identifier)*)) ']' ;

propertyName
    :   Identifier | QualifiedIdentifier
    ;
    
sources
    : 'sources' ':'  sourcedef+
    ;

sourcedef
    :  Identifier ':' object
    ;

Identifier
    :   JavaLetter JavaLetterOrDigit*
    ;

QualifiedIdentifier
    :   Identifier ('.' Identifier)+
    ;
    
MetaName
    : '#'  JavaLetter JavaLetterOrDigit*
    ;

fragment
JavaLetter
    :   [a-zA-Z_] // these are the "java letters" below 0xFF
    |   // covers all characters above 0xFF which are not a surrogate
        ~[\u0000-\u00FF\uD800-\uDBFF]
        {Character.isJavaIdentifierStart(_input.LA(-1))}?
    |   // covers UTF-16 surrogate pairs encodings for U+10000 to U+10FFFF
        [\uD800-\uDBFF] [\uDC00-\uDFFF]
        {Character.isJavaIdentifierStart(Character.toCodePoint((char)_input.LA(-2), (char)_input.LA(-1)))}?
    ;

fragment
JavaLetterOrDigit
    :   [a-zA-Z0-9$_] // these are the "java letters or digits" below 0xFF
    |   // covers all characters above 0xFF which are not a surrogate
        ~[\u0000-\u00FF\uD800-\uDBFF]
        {Character.isJavaIdentifierPart(_input.LA(-1))}?
    |   // covers UTF-16 surrogate pairs encodings for U+10000 to U+10FFFF
        [\uD800-\uDBFF] [\uDC00-\uDFFF]
        {Character.isJavaIdentifierPart(Character.toCodePoint((char)_input.LA(-2), (char)_input.LA(-1)))}?
    ;

literal
    :   integerLiteral
    |   floatingPointLiteral
    |   characterLiteral
    |   stringLiteral
    |   booleanLiteral
    |   nullLiteral
    ;

// §3.10.1 Integer Literals

integerLiteral
    : IntegerLiteral
    ;

IntegerLiteral
    :   DecimalIntegerLiteral
    |   HexIntegerLiteral
    |   OctalIntegerLiteral
    |   BinaryIntegerLiteral
    ;

fragment
DecimalIntegerLiteral
    :   DecimalNumeral IntegerTypeSuffix?
    ;
fragment
HexIntegerLiteral
    :   HexNumeral IntegerTypeSuffix?
    ;
    
fragment
OctalIntegerLiteral
    :   OctalNumeral IntegerTypeSuffix?
    ;

fragment
BinaryIntegerLiteral
    :   BinaryNumeral IntegerTypeSuffix?
    ;

fragment
IntegerTypeSuffix
    :   [lL]
    ;

fragment
DecimalNumeral
    :   '0'
    |   NonZeroDigit (Digits? | Underscores Digits)
    ;

fragment
Digits
    :   Digit (DigitOrUnderscore* Digit)?
    ;

fragment
Digit
    :   '0'
    |   NonZeroDigit
    ;

fragment
NonZeroDigit
    :   [1-9]
    ;

fragment
DigitOrUnderscore
    :   Digit
    |   '_'
    ;

fragment
Underscores
    :   '_'+
    ;

fragment
HexNumeral
    :   '0' [xX] HexDigits
    ;

fragment
HexDigits
    :   HexDigit (HexDigitOrUnderscore* HexDigit)?
    ;

fragment
HexDigit
    :   [0-9a-fA-F]
    ;

fragment
HexDigitOrUnderscore
    :   HexDigit
    |   '_'
    ;

fragment
OctalNumeral
    :   '0' Underscores? OctalDigits
    ;

fragment
OctalDigits
    :   OctalDigit (OctalDigitOrUnderscore* OctalDigit)?
    ;

fragment
OctalDigit
    :   [0-7]
    ;

fragment
OctalDigitOrUnderscore
    :   OctalDigit
    |   '_'
    ;

fragment
BinaryNumeral
    :   '0' [bB] BinaryDigits
    ;

fragment
BinaryDigits
    :   BinaryDigit (BinaryDigitOrUnderscore* BinaryDigit)?
    ;

fragment
BinaryDigit
    :   [01]
    ;

fragment
BinaryDigitOrUnderscore
    :   BinaryDigit
    |   '_'
    ;

// §3.10.2 Floating-Point Literals

floatingPointLiteral: FloatingPointLiteral;

FloatingPointLiteral
    :   DecimalFloatingPointLiteral
    |   HexadecimalFloatingPointLiteral
    ;


DecimalFloatingPointLiteral
    :   Digits '.' Digits? ExponentPart? FloatTypeSuffix?
    |   '.' Digits ExponentPart? FloatTypeSuffix?
    |   Digits ExponentPart FloatTypeSuffix?
    |   Digits FloatTypeSuffix
    ;

fragment
ExponentPart
    :   ExponentIndicator SignedInteger
    ;

fragment
ExponentIndicator
    :   [eE]
    ;

fragment
SignedInteger
    :   Sign? Digits
    ;

fragment
Sign
    :   [+-]
    ;

fragment
FloatTypeSuffix
    :   [fFdD]
    ;

HexadecimalFloatingPointLiteral
    :   HexSignificand BinaryExponent FloatTypeSuffix?
    ;

fragment
HexSignificand
    :   HexNumeral '.'?
    |   '0' [xX] HexDigits? '.' HexDigits
    ;

fragment
BinaryExponent
    :   BinaryExponentIndicator SignedInteger
    ;

fragment
BinaryExponentIndicator
    :   [pP]
    ;

// §3.10.3 Boolean Literals

booleanLiteral
    :   'true'
    |   'false'
    ;

// §3.10.4 Character Literals

characterLiteral: CharacterLiteral;

CharacterLiteral
    :   '\'' SingleCharacter '\'' {setText( String.valueOf(getText().charAt(1)) );}
    |   '\'' EscapeSequence '\''  {setText( loghub.configuration.CharSupport.getStringFromGrammarStringLiteral(getText()));}
    ;

fragment
SingleCharacter
    :   ~['\\]
    ;

stringLiteral
    :   StringLiteral
    ;

patternLiteral
    :   PatternLiteral
    ;


PatternLiteral
    :   '/' PatternFirstCharacter PatternCharacter* '/'
    ;
    
fragment
PatternCharacter
    :   ~[/\\]
    |   EscapeSequence
    ;

fragment
PatternFirstCharacter
    :   ~[/*\\]
    |   EscapeSequence
    ;

StringLiteral
    :   '"' StringCharacter* '"' {setText( loghub.configuration.CharSupport.getStringFromGrammarStringLiteral(getText()));}
    ;


fragment
StringCharacter
    :   ~["\\]
    |   EscapeSequence
    ;

// §3.10.6 Escape Sequences for Character and String Literals

fragment
EscapeSequence
    :   '\\' [btnfr"'\\]
    |   OctalEscape
    |   UnicodeEscape
    ;

fragment
OctalEscape
    :   '\\' OctalDigit
    |   '\\' OctalDigit OctalDigit
    |   '\\' ZeroToThree OctalDigit OctalDigit
    ;

fragment
UnicodeEscape
    :   '\\' 'u' HexDigit HexDigit HexDigit HexDigit
    ;

fragment
ZeroToThree
    :   [0-3]
    ;

nullLiteral
    :   'null'
    ;
//
// Whitespace and comments
//

WS  :  [ \t\r\n\u000C]+ -> skip
    ;

COMMENT
    :   '/*' .*? '*/' -> skip
    ;

LINE_COMMENT
    :   '//' ~[\r\n]* -> skip
    ;
