//
// A mix of DSL grammar, Java grammar and Groovy grammar
// Java grammar is taken from https://github.com/antlr/grammars-v4/blob/master/java/Java.g4

grammar Route;

@parser::header {
import loghub.configuration.GrammarParserFiltering;
import loghub.configuration.GrammarParserFiltering.BEANTYPE;
import loghub.configuration.GrammarParserFiltering.SECTION;
}
@parser::members {
    String lambdaVariable;
    SECTION currentSection;
    boolean inSection(SECTION s) {
        return s==currentSection;
    }
    boolean inLambda() {
        return lambdaVariable != null;
    }
    public GrammarParserFiltering filter;
}

configuration: (pipeline|input|output|sources|property)+ EOF;

pipeline
    : {currentSection=SECTION.PIPELINE;} 'pipeline' '[' identifier ']' '{' pipenodeList? '}' ( '|' '$' finalpiperef) ? {currentSection=null;}
    ;

input
    : {currentSection=SECTION.INPUT;}'input' '{' inputObjectlist '}' ('|' '$' piperef)? {currentSection=null;}
    ;

output
    : {currentSection=SECTION.OUTPUT;} 'output' ('$' piperef '|' )? '{' outputObjectlist '}' {currentSection=null;}
    ;

inputObjectlist: (object (',' object)*)? ','?;

outputObjectlist: (object (',' object)*)? ','?;
pipenodeList: ( (pipenode | '+' forkpiperef ) (('+' forkpiperef)|('|' pipenode))*) ('>' forwardpiperef)?
              |  ('>' forwardpiperef)
    ;

forkpiperef: '$' identifier;

forwardpiperef: '$' identifier;

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
    | foreach
    ;

object
    : QualifiedIdentifier {filter.enterObject($QualifiedIdentifier.text);} beansDescription? {filter.exitObject();}
    ;

implicitObject
    : beansDescription
    ;

beansDescription
    : '{' (bean (',' bean)*)? ','? '}'
    ;

// All defined bean names must be replicated as identifier
bean
    : {inSection(SECTION.INPUT)}?    (bn='decoder' ':' object)
    | {inSection(SECTION.PIPELINE)}? (bn='if' ':' expression)
    | {inSection(SECTION.PIPELINE)}? (bn=('success' | 'failure' | 'exception') ':' pipenode)
    | {inSection(SECTION.PIPELINE)}? (bn='fields' ':' array)
    | {inSection(SECTION.PIPELINE)}? (bn='destinationTemplate' ':' stringLiteral)
    | {inSection(SECTION.OUTPUT)}?   (bn='encoder' ':' object)
    | (bn='sslContext' ':' {filter.enterImplicitObject($bn.text);} beanValue {filter.exitImplicitObject($ctx.beanValue());})
    | (bn='sslParams' ':' {filter.enterImplicitObject($bn.text);} beanValue {filter.exitImplicitObject($ctx.beanValue());})
    | (beanName ':' beanValue {filter.cleanBeanType();})
    ;

beanName
    : identifier {filter.resolveBeanType($identifier.text);}
    ;

beanValue
    : nullLiteral
    | {filter.allowedBeanType(BEANTYPE.VARIABLE_PATH)}? eventVariableBean
    | {filter.allowedBeanType(BEANTYPE.OBJECT)}? object
    | {filter.allowedBeanType(BEANTYPE.INTEGER)}? integerLiteral
    | {filter.allowedBeanType(BEANTYPE.IMPLICIT_OBJECT)}? implicitObject
    | {filter.allowedBeanType(BEANTYPE.FLOAT)}? floatingPointLiteral
    | {filter.allowedBeanType(BEANTYPE.CHARACTER)}? characterLiteral
    | {filter.allowedBeanType(BEANTYPE.STRING)}? stringLiteral
    | {filter.allowedBeanType(BEANTYPE.BOOLEAN)}? booleanLiteral
    | {filter.allowedBeanType(BEANTYPE.SECRET)}? secret
    | {filter.allowedBeanType(BEANTYPE.LAMBDA)}? lambda
    | {filter.allowedBeanType(BEANTYPE.EXPRESSION)}? expression
    | {filter.allowedBeanType(BEANTYPE.VARIABLE_PATH_ARRAY)}? vparray
    | {filter.allowedBeanType(BEANTYPE.ARRAY)}? array
    | {filter.allowedBeanType(BEANTYPE.OPTIONAL_ARRAY)}? (stringLiteral | array)
    | {filter.allowedBeanType(BEANTYPE.MAP)}? map
    ;

eventVariableBean
    : (fsv=stringLiteral | fev=eventVariable)
    ;

finalpiperef: piperef;

piperef:  identifier;

merge
    : 'merge' '{' (mergeArgument (',' mergeArgument)*)? ','? '}'
    ;

mergeArgument
    : type='if' ':' expression
    | type='index' ':' expression
    | type='seeds' ':' map
    | type='doFire' ':' expression
    | type='onFire' ':' pipenode
    | type='onExpiration' ':' pipenode
    | type='expiration' ':' (integerLiteral | floatingPointLiteral)
    | type='forward' ':' booleanLiteral
    | type='default' ':' beanValue
    | type='inPipeline' ':' stringLiteral
    | type='defaultMeta' ':' beanValue
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
    : 'log' '(' expression ',' level ')'
    ;

level
    : 'FATAL'
    | 'ERROR'
    | 'WARN'
    | 'INFO'
    | 'DEBUG'
    | 'TRACE'
    ;

property
    : (pn='http.sslContext' ':' {filter.enterImplicitObject($pn.text);} beanValue {filter.exitImplicitObject($ctx.beanValue());})
    | (pn='http.sslParams' ':' {filter.enterImplicitObject($pn.text);} beanValue {filter.exitImplicitObject($ctx.beanValue());})
    | (pn='jmx.sslContext' ':' {filter.enterImplicitObject($pn.text);} beanValue {filter.exitImplicitObject($ctx.beanValue());})
    | (pn='jmx.sslParams' ':' {filter.enterImplicitObject($pn.text);} beanValue {filter.exitImplicitObject($ctx.beanValue());})
    | (pn='plugins' ':' {filter.checkProperty($pn.text);} beanValue {filter.cleanBeanType(); filter.refreshIncludes($ctx.beanValue());})
    | (propertyName ':' {filter.checkProperty($propertyName.text);} beanValue {filter.cleanBeanType();})
    ;

propertyName
    : qualifiedIdentifier
    | identifier
    ;

etl
    : eventVariable op='<' s=eventVariable
    | eventVariable op='-'
    | eventVariable op='=' eventVariable
    | eventVariable op='=' (sl = stringLiteral | nl = nullLiteral | c = characterLiteral | l = nonStringliteral)
    | eventVariable op='=' expression
    | eventVariable op='=+' expression
    | eventVariable op='@' e=expression map
    | op='(' QualifiedIdentifier ')' eventVariable
    ;

path
    : 'path' eventVariable pipenode
    ;
 
foreach
    : 'foreach' eventVariable pipenode
    ;

test: testExpression '?' (pipenode | '>' forwardpiperef | '+' forkpiperef ) (':' (pipenode | '>' forwardpiperef | '+' forkpiperef ))? ;

testExpression: expression;

secret: '*' id=Identifier ('.' SecretAttribute )? ;

SecretAttribute: 'text' | 'blob';

lambda: identifier {lambdaVariable = $identifier.text;} '->' expression {lambdaVariable = null;};

// The rules from https://groovy-lang.org/operators.html#_operator_precedence needs to be explicited
// because expressions are rewritten
expression
    :   sl = stringLiteral ( expressionsList )?
    |   nl = nullLiteral
    |   c = characterLiteral
    |   l = nonStringliteral
    |   ev = eventVariable
    |   '(' convertclass=qualifiedIdentifier ')' expression
    |   'new' newclass = qualifiedIdentifier ('(' ')' | expressionsList)
    |   opnotlogical='!' e1=expression
    |   opnotbinary='.~' e1=expression
    |   e1 = expression opinfix='**' e2=expression
    |   op3=('+'|'-') e1=expression
    |   e1 = expression opinfix=('*'|'/'|'%') e2=expression
    |   e1 = expression opinfix=('+'|'-') e2=expression
    |   e1 = expression opinfix=('<<'|'>>'|'>>>') e2=expression
    |   e1 = expression opcomp=('<'|'<='|'>'|'>=') e2=expression
    |   e1 = expression opin=('in'|'!in') ((('set' | 'list')? el=expressionsList) | e2=expression)
    |   e1 = expression (neg='!')? opinstance='instanceof' qi=qualifiedIdentifier
    |   e1 = expression opinstance='!instanceof' qi=qualifiedIdentifier
    |   (exists = eventVariable op=('=='|'!=') '*' | '*' op=('=='|'!=') exists = eventVariable)
    |   e1 = expression opcomp=('=='|'!='|'<=>') e2=expression
    |   e1 = expression opcomp=('==='|'!==') e2=expression
    |   e1 = expression opm=matchOperator pattern
    |   e1 = expression opbininfix='.&' e2=expression
    |   e1 = expression opbininfix='.^' e2=expression
    |   e1 = expression opbininfix='.|' e2=expression
    |   e1 = expression op12='&&' e2=expression
    |   e1 = expression op13='||' e2=expression
    |   '(' e3 = expression ')'
    |   expression '[' arrayIndexSign='-'? arrayIndex=integerLiteral ']'
    |   stringFunction = (Trim | Capitalize | IsBlank | Normalize | Uncapitalize | Lowercase | Uppercase) '(' expression ')'
    |   join = Join '(' stringLiteral ',' expression  ')'
    |   split = Split '(' (pattern | stringLiteral) ',' expression ')'
    |   gsub = 'gsub' '(' expression ',' pattern ',' stringLiteral ')'
    |   now = 'now'
    |   isEmpty = 'isEmpty' '(' expression ')'
    |   isIp = 'isIP' '(' expression ')'
    |   collection=('set' | 'list') ('(' ')' | expressionsList)
    |   {inLambda()}? lambdavar=Identifier {lambdaVariable.equals($lambdavar.text)}?
    ;

Trim: 'trim';
Capitalize: 'capitalize';
Uncapitalize: 'uncapitalize';
IsBlank: 'isBlank';
Normalize: 'normalize';
Lowercase: 'lowercase';
Uppercase: 'uppercase';
Join: 'join';
Split: 'split';

expressionsList
    : '(' expression ( ','  expression )* ','? ')'
    ;

matchOperator
    :   '=~'
    |   '==~'
    ;

vparray
    : '[' (eventVariable ( ',' eventVariable)* ','?) ?']'
    | '[' (stringLiteral ( ',' stringLiteral)* ','?) ?']'
    ;

array
    : {filter.cleanBeanType();}  '[' arrayContent ']'
    | source
    ;

arrayContent:
    ','
    | ((beanValue ( ',' beanValue)*)? ','?)
    ;

map
    : {filter.cleanBeanType();} '{' (literal ':' beanValue ( ',' ? literal ':' beanValue)*)? ','? '}'
    | source
    ;

source
    : '%' identifier
    ;

eventVariable: '[' (ts='@timestamp' | lex='@lastException' | (ctx='@context' '.'? (vp1=varPath)?) | (indirect='<-'? (MetaName | (root='.'? vp2=varPath))) | vproot=('.' | '^')) ']' ;

varPath: (pathElement pathElement*) | QualifiedIdentifier;

pathElement: identifier | StringLiteral ;

sources
    : 'sources' ':'  sourcedef+
    ;

sourcedef
    :  identifier ':' object
    ;

identifier
    : 'index' | 'seeds' | 'doFire' | 'onFire' | 'expiration' | 'forward' | 'default' | 'merge' | 'inPipeline' | 'path' | 'foreach' | 'bean' | 'field' | 'input' | 'in' | 'decoder'
    | 'sslContext' | 'sslParams'
    | 'if' | 'success' | 'failure' | 'exception' | 'field' | 'fields' | 'destination' | 'destinationTemplate' | 'encoder' | 'log' | 'fire' | 'pipeline' | 'output' | 'onExpiration'
    | 'defaultMeta' | 'map' | 'plugins'
    | 'FATAL' | 'ERROR' | 'WARN' | 'INFO' | 'DEBUG' | 'TRACE'
    | 'new' | 'instanceof' | 'now' | 'isEmpty' | 'isIP'
    | 'sources' | 'true' | 'false' | 'null' | 'drop'
    | 'trim' | 'capitalize' | 'uncapitalize' | 'isBlank' | 'normalize' | 'lowercase' | 'uppercase' | 'split' | 'join' | 'gsub'
    | 'text' | 'blob'
    | 'set' | 'list'
    | Identifier
    ;

Identifier
    :   JavaLetter JavaLetterOrDigit*
    ;

qualifiedIdentifier
    : QualifiedIdentifier
    ;

QualifiedIdentifier
    :   Identifier ('.' Identifier)+
    ;
    
MetaName
    : '#'  JavaLetter JavaLetterOrDigit*
    ;

fragment
JavaLetter
    :    [a-zA-Z_] // these are the "java letters" below 0x7F, minus $
    |    // covers all characters above 0x7F which are not a surrogate
        ~[\u0000-\u007F\uD800-\uDBFF]
        { Character.isJavaIdentifierStart(_input.LA(-1)) }?
    |    // covers UTF-16 surrogate pairs encodings for U+10000 to U+10FFFF
        [\uD800-\uDBFF] [\uDC00-\uDFFF]
        { Character.isJavaIdentifierStart(Character.toCodePoint((char)_input.LA(-2), (char)_input.LA(-1))) }?
    ;

fragment
JavaLetterOrDigit
    :    [a-zA-Z0-9$_] // these are the "java letters" below 0x7F
    |    // covers all characters above 0x7F which are not a surrogate
        ~[\u0000-\u007F\uD800-\uDBFF]
        { Character.isJavaIdentifierPart(_input.LA(-1)) }?
    |    // covers UTF-16 surrogate pairs encodings for U+10000 to U+10FFFF
        [\uD800-\uDBFF] [\uDC00-\uDFFF]
        { Character.isJavaIdentifierPart(Character.toCodePoint((char)_input.LA(-2), (char)_input.LA(-1))) }?
    ;

literal
    :   integerLiteral
    |   floatingPointLiteral
    |   characterLiteral
    |   stringLiteral
    |   booleanLiteral
    |   nullLiteral
    ;

nonStringliteral
    :   integerLiteral
    |   floatingPointLiteral
    |   booleanLiteral
    |   nullLiteral
    ;

// §3.10.1 Integer Literals

integerLiteral
    :   DecimalDigits
    |   HexDigits
    |   OctalDigits
    |   BinaryDigits
    ;

DecimalDigits
    :   Digits
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
    :   ('0x' | '0X') HexDigits
    ;

HexDigits
    :   ('0x' | '0X') HexDigit (HexDigitOrUnderscore* HexDigit)?
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

OctalDigits
    :   ('0o' | '0o') OctalDigit (OctalDigitOrUnderscore* OctalDigit)?
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

BinaryDigits
    :   ('0b' | '0B') BinaryDigit (BinaryDigitOrUnderscore* BinaryDigit)?
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

pattern
    :   Pattern
    ;

Pattern
    :  '/' PatternCharacter+? '/'
    |   '"""' .+? '"""'
    ;

fragment
PatternCharacter
    :   '\\' [\r\n] [\r\n]?   // A unix-macos-windows line feed needs to be escaped
    |   '\\/'
    |   ~[\r\n]
    ;

StringLiteral
    :   '"' StringCharacter* '"' {setText(loghub.configuration.CharSupport.getStringFromGrammarStringLiteral(getText()));}
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
