package edu.nyu.cs.cs2580;

/**
 * Document with score.
 * 
 * @author fdiaz
 * @author congyu
 */
class ScoredDocument implements Comparable<ScoredDocument> {
  private Document _doc;
  private double _score;
  private float pageRank;
  private int numview;

  public ScoredDocument(Document doc, double score) {
    _doc = doc;
    _score = score;
    pageRank = doc.getPageRank();
    numview = doc.getNumViews();
  }

  public String asTextResult() {
    StringBuffer buf = new StringBuffer();
    buf.append(_doc._docid).append("\t");
    buf.append(_doc.getTitle()).append("\t");
    buf.append(_score);
    buf.append("\t").append(pageRank);
    buf.append("\t").append(numview);
    return buf.toString();
  }

  /**
   * @CS2580: Student should implement {@code asHtmlResult} for final project.
   */
  public String asHtmlResult() {
    return "";
  }

  public Document getDocument() {
    return this._doc;
  }

  public int getDocumentID() {
    return this._doc._docid;
  }

  @Override
  public int compareTo(ScoredDocument o) {
    if (this._score == o._score) {
      return 0;
    }
    return (this._score > o._score) ? 1 : -1;
  }

  public float getPageRank() {
    return pageRank;
  }

  public void setPageRank(float pageRank) {
    this.pageRank = pageRank;
  }

  public int getNumview() {
    return numview;
  }

  public void setNumview(int numview) {
    this.numview = numview;
  }
}
