// $antlr-format alignTrailingComments true, columnLimit 150, minEmptyLines 1, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine false, allowShortBlocksOnASingleLine true, alignSemicolons hanging, alignColons hanging

grammar Xdr;

// parser rules

specification
    : definition+
    ; //this is the top level rule for xdr (rfc 4506)

definition
    : 'typedef' typeBody ';'                       # typeDef
    | 'enum' IDENTIFIER enumBody ';'?              # enumDef
    | 'struct' IDENTIFIER structBody ';'?          # structDef
    | 'union' IDENTIFIER unionBody ';'?            # unionDef
    | 'const' IDENTIFIER '=' constant ';'          # constantDef
    ;

declaration
    : typeSpecifier IDENTIFIER
    | typeSpecifier IDENTIFIER array='[' value ']'
    | typeSpecifier IDENTIFIER array='<' value? '>'
    | typeSpecifier '<' value? '>' IDENTIFIER
    | qualifier='opaque' IDENTIFIER '[' value ']'
    | qualifier='opaque' IDENTIFIER '<' value? '>'
    | qualifier='string' IDENTIFIER '<' value? '>'
    | typeSpecifier optional='*' IDENTIFIER
    | 'void'
    ;

value
    : constant
    | IDENTIFIER
    ;

constant
    : DECIMAL
    | HEXADECIMAL
    | OCTAL
    ;

typeSpecifier
    : nativeType
    | subTypeDefinition
    | IDENTIFIER
    ;
nativeType
    : 'unsigned'? 'int'
    | 'unsigned'? 'hyper'
    | 'float'
    | 'double'
    | 'quadruple'
    | 'bool'
    ;

subTypeDefinition
    : enumTypeSpec
    | structTypeSpec
    | unionTypeSpec
    ;

enumTypeSpec
    : 'enum' enumBody
    ;

typeBody
    : declaration
    ;

enumBody
    : '{' enumEntry ((','|';') enumEntry)* (','|';')? '}'
    ;

enumEntry
    : (IDENTIFIER '=' value)
    ;

structTypeSpec
    : 'struct' structBody
    ;

structBody
    : '{' (declaration) (';' declaration)* ';'? '}'
    ;

unionTypeSpec
    : 'union' unionBody
    ;

unionBody
    : 'switch'? '(' (IDENTIFIER | declaration) ')' '{' caseSpec caseSpec* ('default' ':' declaration ';')? '}'
    ;

caseSpec
    : ('case' value ':') ('case' value ':')* (IDENTIFIER | declaration) ';'
    ;

// lexer rules

COMMENT
    : '/*' .*? '*/' -> skip
    ;

OCTAL
    : '0' [1-7] ([0-7])*
    ;

DECIMAL
    : ('-')? ([0-9])+
    ;

HEXADECIMAL
    : '0x' ([a-fA-F0-9])+
    ;

IDENTIFIER
    : [a-zA-Z] ([a-zA-Z0-9_])*
    ;

WS
    : [ \t\r\n]+ -> skip
    ;
