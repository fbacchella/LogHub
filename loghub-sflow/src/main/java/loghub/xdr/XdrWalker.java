package loghub.xdr;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lombok.ToString;

public class XdrWalker extends XdrBaseListener {
    static final class TypedStack extends ArrayDeque<Object> {
        public <T> T popTyped() {
            @SuppressWarnings("unchecked")
            T temp = (T) pop();
            return temp;
        }
    }

    private enum StackMarker {
        TYPE_DEF,
        ENUM_DEF,
        STRUCT_DEF,
        UNION_DEF,
        CONSTANT_DEF,
    }

    @ToString
    private static class Declaration {
        boolean isVariable = false;
        boolean isFixed = false;
        boolean isVoid = false;
        boolean isArray = false;
        boolean isOptional = false;
        int size = -1;
        TypeSpecifier<?> type;
        String identifier;
    }

    private static final Logger logger = LogManager.getLogger();

    private final TypedStack stack = new TypedStack();

    private final Map<String, TypeSpecifier<?>> types = new HashMap<>();
    private final Map<String, Number> constants = new HashMap<>();

    private final ParseTreeWalker walker = new ParseTreeWalker();

    public Map<String, TypeSpecifier<?>> startWalk(CharStream stream, Map<String, TypeSpecifier<?>> previousTypes) {
        types.putAll(previousTypes);
        XdrLexer lexer = new XdrLexer(stream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        XdrParser parser = new XdrParser(tokens);
        walker.walk(this, parser.specification());
        assert stack.isEmpty();
        return types;
    }

    @Override
    public void enterTypeDef(XdrParser.TypeDefContext ctx) {
        stack.push(StackMarker.TYPE_DEF);
    }

    @Override
    public void exitTypeDef(XdrParser.TypeDefContext ctx) {
        Declaration d = stack.popTyped();
        if (! d.isVoid) {
            types.put(d.identifier, d.type);
        }
        stack.pop();
    }

    @Override
    public void enterEnumDef(XdrParser.EnumDefContext ctx) {
        stack.push(StackMarker.ENUM_DEF);
        Map<Number, String> enumDef = new HashMap<>();
        for (XdrParser.EnumEntryContext e: ctx.enumBody().enumEntry()) {
            enumDef.put(resolveValue(e.value()), e.IDENTIFIER().getText());
        }
        types.put(ctx.IDENTIFIER().getText(), new EnumTypeSpecifier(ctx.IDENTIFIER().getText(), enumDef));
    }

    @Override
    public void exitEnumDef(XdrParser.EnumDefContext ctx) {
        stack.pop();
    }

    @Override
    public void enterStructDef(XdrParser.StructDefContext ctx) {
        stack.push(StackMarker.STRUCT_DEF);
    }

    @Override
    public void exitStructDef(XdrParser.StructDefContext ctx) {
        LinkedHashMap<String, TypeSpecifier<?>> members = new LinkedHashMap<>();
        while (StackMarker.STRUCT_DEF != stack.peek()) {
            Declaration d = stack.popTyped();
            members.put(d.identifier, d.type);
        }
        types.put(ctx.IDENTIFIER().getText(), new StructSpecifier(ctx.IDENTIFIER().getText(), members));
        stack.pop();
    }

    @Override
    public void enterUnionDef(XdrParser.UnionDefContext ctx) {
        stack.push(StackMarker.UNION_DEF);
    }

    @Override
    public void exitUnionDef(XdrParser.UnionDefContext ctx) {
        while (stack.pop() != StackMarker.UNION_DEF) {
            // empty loop
        }
        types.put(ctx.IDENTIFIER().getText(), UnionTypeSpecifier.of(ctx.IDENTIFIER().getText()));
    }

    @Override
    public void enterConstantDef(XdrParser.ConstantDefContext ctx) {
        stack.push(StackMarker.CONSTANT_DEF);
        constants.put(ctx.IDENTIFIER().getText(), Long.parseLong(ctx.constant().getText()));
    }

    @Override
    public void exitConstantDef(XdrParser.ConstantDefContext ctx) {
        stack.pop();
    }

    /**
     * {@inheritDoc}
     *
     * <p>The default implementation does nothing.</p>
     *
     * @param ctx
     */
    @Override
    public void enterDeclaration(XdrParser.DeclarationContext ctx) {
        Declaration declaration = new Declaration();
        boolean isString = false;
        boolean isOpaque = false;
        for (ParseTree children: ctx.children) {
            switch (children.getText()) {
            case "opaque":
                isOpaque = true;
                break;
            case "[":
                declaration.isFixed = true;
                break;
            case "void":
                declaration.isVoid = true;
                break;
            case "string":
                isString = true;
                break;
            }
        }
        if (ctx.value() != null) {
            declaration.size = resolveValue(ctx.value()).intValue();
        }
        declaration.isArray = ctx.array != null;
        declaration.isOptional = ctx.optional != null;
        if (! declaration.isVoid) {
            declaration.identifier = ctx.IDENTIFIER().getText();
        }
        if (isString && ! declaration.isFixed) {
            declaration.type = new NativeTypeSpecifier(NativeType.STRING, declaration.size);
        } else if (isString) {
            declaration.type = new FixedStringTypeSpecifier(declaration.size);
        } else if (isOpaque && declaration.isFixed) {
            declaration.type = new FixedOpaqueTypeSpecifier(declaration.size);
        } else if (isOpaque) {
            declaration.type = new NativeTypeSpecifier(NativeType.BYTE_ARRAY, declaration.size);
        } else if (ctx.typeSpecifier() != null) {
            declaration.type = resolveTypeSpecifier(declaration, ctx.typeSpecifier());
        }
        stack.push(declaration);
    }

    private Number resolveValue(XdrParser.ValueContext ctx) {
        if (ctx.constant() != null) {
            return Long.decode(ctx.constant().getText());
        } else {
            return constants.get(ctx.IDENTIFIER().getText());
        }
    }

    private TypeSpecifier<?> resolveTypeSpecifier(Declaration declaration, XdrParser.TypeSpecifierContext ctx) {
        if (ctx.nativeType() != null) {
            NativeType nt = resolveNativeType(ctx.nativeType());
            return new NativeTypeSpecifier(nt, Math.max(nt.size, declaration.size));
        } else if (ctx.IDENTIFIER() != null) {
            String identifier = ctx.IDENTIFIER().getText();
            if (types.containsKey(identifier)) {
                return new IndirectTypeSpecifier(ctx.IDENTIFIER().getText(), types.get(ctx.IDENTIFIER().getText()));
            } else {
                logger.error("A declaration uses the undefined type {}", identifier);
                return new InvalidTypeSpecifier(identifier, declaration.identifier);
            }
        } else {
            logger.error("Unusable declaration {}", declaration);
            return new InvalidTypeSpecifier("empty declaration", declaration.identifier);
        }
    }

    private NativeType resolveNativeType(XdrParser.NativeTypeContext ctx) {
        boolean unsigned = "unsigned".equals(ctx.children.get(0).getText());
        int nativeTypeRank = unsigned ? 1 : 0;
        String nativeType = ctx.children.get(nativeTypeRank).getText();
        switch (nativeType) {
        case "int": return unsigned ? NativeType.UNSIGNED_INT : NativeType.INT;
        case "hyper": return unsigned ? NativeType.UNSIGNED_HYPER : NativeType.HYPER;
        case "float": return NativeType.FLOAT;
        case "double": return NativeType.DOUBLE;
        case "quadruple": return NativeType.QUADRUPLE;
        case "bool": return NativeType.BOOL;
        default: throw new IllegalStateException();
        }
    }

}
