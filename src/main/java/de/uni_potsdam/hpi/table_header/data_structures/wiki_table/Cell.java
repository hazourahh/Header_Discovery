package de.uni_potsdam.hpi.table_header.data_structures.wiki_table;

import java.util.ArrayList;
import java.util.List;
/**
 * @author Hazar Harmouch
 *
 */
public class Cell {

    private int cellID;
    private List<String> textTokens=new ArrayList<>();
    private String text;
    private String tdHtmlString;
    private List<Surface_Link> surfaceLinks=new ArrayList<>();
    private int subtableID;
    private boolean isNumeric;

    public Cell() {
        super();
    }

    public int getCellID() {
        return cellID;
    }

    public List<String> getTextTokens() {
        return textTokens;
    }

    String getText() { return text; }

    public String getTdHtmlString() {
        return tdHtmlString;
    }

    public List<Surface_Link> getSurfaceLinks() {
        return surfaceLinks;
    }

    public int getSubtableID() {
        return subtableID;
    }

    public boolean isNumeric() {
        return isNumeric;
    }

    public void setCellID(int cellID) {
        this.cellID = cellID;
    }

    public void setTextTokens(List<String> textTokens) {
        this.textTokens = textTokens;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setTdHtmlString(String tdHtmlString) {
        this.tdHtmlString = tdHtmlString;
    }

    public void setSurfaceLinks(List<Surface_Link> surfaceLinks) {
        this.surfaceLinks = surfaceLinks;
    }

    public void setSubtableID(int subtableID) {
        this.subtableID = subtableID;
    }

    public void setNumeric(boolean numeric) {
        isNumeric = numeric;
    }
}
