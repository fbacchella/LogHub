package boxplot;

import org.apache.batik.svggen.DOMGroupManager;
import org.apache.batik.svggen.ExtensionHandler;
import org.apache.batik.svggen.ImageHandler;
import org.apache.batik.svggen.SVGGeneratorContext;
import org.apache.batik.svggen.SVGGraphics2D;
import org.w3c.dom.Document;

public class SVGGraphics extends SVGGraphics2D {
  public SVGGraphics(Document domFactory) {
    super(domFactory);
  }

  public SVGGraphics(Document domFactory, ImageHandler imageHandler, ExtensionHandler extensionHandler, boolean textAsShapes) {
    super(domFactory, imageHandler, extensionHandler, textAsShapes);
  }

  public SVGGraphics(SVGGeneratorContext generatorCtx, boolean textAsShapes) {
    super(generatorCtx, textAsShapes);
  }

  public SVGGraphics(SVGGraphics2D g) {
    super(g);
  }

  public DOMGroupManager domGroupManager() {
    return domGroupManager;
  }

  public SVGGeneratorContext generatorCtx() {
    return generatorCtx;
  }
}
