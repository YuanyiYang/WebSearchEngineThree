package edu.nyu.cs.cs2580;

/**
 * @CS2580: implement this class for HW2 to incorporate any additional
 * information needed for your favorite ranker.
 */
public class DocumentIndexed extends Document {
  private static final long serialVersionUID = 9184892508124423115L;

  public DocumentIndexed(int docid) {
    super(docid);
  }
  
  /*
   * //Basic information for display protected String _title = ""; protected
   * String _url = "";
   * 
   * // Basic information for ranking protected float _pageRank = 0.0f;
   * protected int _numViews = 0;
   */

  private double body_freq;

  public void setBodyFrequency(int bf) {
    body_freq = (double) bf;
  }

  public double getBodyFrequency() {
    return body_freq;
  }
}
