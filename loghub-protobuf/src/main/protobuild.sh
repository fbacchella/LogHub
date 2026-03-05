set -x
chmod a+rx "$PROTOC_BIN"
mkdir -p "$DESCRIPTOR_OUT_DIR"
for f in $PROTO_SRC_DIR/* ; do
    name=$(basename "$f" .proto)
    dir=$(dirname "$f")
    "$PROTOC_BIN" \
        --descriptor_set_out="$DESCRIPTOR_OUT_DIR/${name}.binpb" \
        --include_imports \
        --proto_path="$PROTO_SRC_DIR" \
        -I$PROTOC_INCLUDES \
        -I$PROTO_SRC_DIR \
         $(find $f -name '*.proto')
done
