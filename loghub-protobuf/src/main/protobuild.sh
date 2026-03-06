set -x
chmod a+rx "$PROTOC_BIN"
mkdir -p "$DESCRIPTOR_OUT_DIR"
for name in prometheus prometheus2 opentelemetry centreon ping ; do
    case $name in
    prometheus)
        finddir=prometheus
        ;;
    opentelemetry)
        finddir=opentelemetry
        ;;
    prometheus2)
        finddir=io/prometheus
        ;;
    centreon)
        finddir=com/centreon
        ;;
    ping)
        finddir=ping
        ;;
    esac
    if [ -e "$PROTO_SRC_DIR/$finddir" ] ; then
        "$PROTOC_BIN" \
            --descriptor_set_out="$DESCRIPTOR_OUT_DIR/${name}.binpb" \
            --include_imports \
            --proto_path="$PROTO_SRC_DIR" \
            -I$PROTOC_INCLUDES \
            -I$PROTO_SRC_DIR \
             $(find $PROTO_SRC_DIR/$finddir -name '*.proto')
    fi
done
