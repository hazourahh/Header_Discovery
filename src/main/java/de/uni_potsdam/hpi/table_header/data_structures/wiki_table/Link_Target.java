package de.uni_potsdam.hpi.table_header.data_structures.wiki_table;
/**
 * @author Hazar Harmouch
 * This class represent a link target of a webtable cell
 */
public class Link_Target {

    private long id;
    private String language;
    private String title;
    private boolean redirecting;
    private int namesapce;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public boolean isRedirecting() {
        return redirecting;
    }

    public void setRedirecting(boolean redirecting) {
        this.redirecting = redirecting;
    }

    public int getNamesapce() {
        return namesapce;
    }

    public void setNamesapce(int namesapce) {
        this.namesapce = namesapce;
    }
}
