chmod a+rx "$PROTOC_BIN"
mkdir -p "$DESCRIPTOR_OUT_DIR"
for f in $(cd "$PROTO_SRC_DIR" && find * -name "*.proto") ; do
    name=$(basename "$f" .proto)
    dir=$(dirname "$f")
    mkdir -p "$DESCRIPTOR_OUT_DIR/${dir}"
    "$PROTOC_BIN" \
        --descriptor_set_out="$DESCRIPTOR_OUT_DIR/${dir}/${name}.binpb" \
        --include_imports \
        --proto_path="$PROTO_SRC_DIR" \
        -I$PROTOC_INCLUDES \
        "$PROTO_SRC_DIR/$f"
done
