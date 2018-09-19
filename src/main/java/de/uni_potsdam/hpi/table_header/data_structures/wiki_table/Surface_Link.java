package de.uni_potsdam.hpi.table_header.data_structures.wiki_table;
/**
 * @author Hazar Harmouch
 *
 */
public class Surface_Link {

    private  int offset;
    private int endOffset;
    private boolean isInTemplate;
    private String locType;
    private Link_Target target;
    private String surface;
    private String linkType;
    private boolean inTemplate;

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(int endOffset) {
        this.endOffset = endOffset;
    }

    public boolean isInTemplate() {
        return isInTemplate;
    }

    public void setInTemplate(boolean inTemplate) {
        isInTemplate = inTemplate;
    }

    public String getLocType() {
        return locType;
    }

    public void setLocType(String locType) {
        this.locType = locType;
    }

    public Link_Target getTarget() {
        return target;
    }

    public void setTarget(Link_Target target) {
        this.target = target;
    }

    public String getSurface() {
        return surface;
    }

    public void setSurface(String surface) {
        this.surface = surface;
    }

    public String getLinkType() {
        return linkType;
    }

    public void setLinkType(String linkType) {
        this.linkType = linkType;
    }
}
