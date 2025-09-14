package loghub.commands;

import com.beust.jcommander.Parameter;

import lombok.Getter;

@Getter
public class BaseParameters {

    @Parameter(names = {"--help", "-h"}, help = true)
    private boolean help = false;

    void reset() {
        help = false;
    }

}
