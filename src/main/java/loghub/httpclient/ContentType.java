package loghub.httpclient;

import lombok.Getter;

@Getter
public enum ContentType {

    TEXT_HTML(true, AbstractHttpClientService.TEXT_HTML),
    TEXT_PLAIN(true, AbstractHttpClientService.TEXT_PLAIN),
    APPLICATION_OCTET_STREAM(false, AbstractHttpClientService.APPLICATION_OCTET_STREAM),
    APPLICATION_JSON(true, AbstractHttpClientService.APPLICATION_JSON),
    APPLICATION_XML(true, AbstractHttpClientService.APPLICATION_XML);

    ContentType(boolean textBody, String mimeType) {
        this.textBody = textBody;
        this.mimeType = mimeType;
    }
    private final boolean textBody;
    private final String mimeType;

}
