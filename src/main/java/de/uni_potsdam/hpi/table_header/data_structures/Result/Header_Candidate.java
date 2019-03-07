package de.uni_potsdam.hpi.table_header.data_structures.Result;

/**
 * @author Hazar Harmouch
 */

public class Header_Candidate extends Candidate {
    private String header;

    public Header_Candidate(String header, double similarity_score) {
        super(similarity_score);
        this.header = header;

    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    @Override
    public String toString() {
        return "(" + header + "," + getSimilarity_score() + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!Header_Candidate.class.isAssignableFrom(obj.getClass())) {
            return false;
        }

        final Header_Candidate other = (Header_Candidate) obj;
        if ((this.header == null) ? (other.header != null) : !this.header.equals(other.header)) {
            return false;
        }

        if (!this.header.equals(other.header)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return this.hashCode();
    }

}
