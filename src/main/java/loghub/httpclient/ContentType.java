package loghub.httpclient;

public enum ContentType {

    TEXT_HTML {
        @Override
        public String getMimeType() {
            return AbstractHttpClientService.TEXT_HTML;
        }
    },
    TEXT_PLAIN {
        @Override
        public String getMimeType() {
            return AbstractHttpClientService.TEXT_PLAIN;
        }
    },
    APPLICATION_OCTET_STREAM {
        @Override
        public String getMimeType() {
            return AbstractHttpClientService.APPLICATION_OCTET_STREAM;
        }
    },
    APPLICATION_JSON {
        @Override
        public String getMimeType() {
            return AbstractHttpClientService.APPLICATION_JSON;
        }
    },
    APPLICATION_XML {
        @Override
        public String getMimeType() {
            return AbstractHttpClientService.APPLICATION_XML;
        }
    };
    public abstract String getMimeType();
}
