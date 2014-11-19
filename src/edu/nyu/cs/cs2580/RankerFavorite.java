package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2 based on a refactoring of your favorite
 *          Ranker (except RankerPhrase) from HW1. The new Ranker should no
 *          longer rely on the instructors' {@link IndexerFullScan}, instead it
 *          should use one of your more efficient implementations.
 */
public class RankerFavorite extends Ranker {

  private long total_tf; // total term frequency in the collection
  private double smooth;

  public RankerFavorite(Options options, CgiArguments arguments, Indexer indexer) {
    super(options, arguments, indexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
    total_tf = _indexer.totalTermFrequency();
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults) {

    _indexer.buildInvertMap(query);
    Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
    QueryPhrase qp = new QueryPhrase(query._query);
    // numebr of times a word or phrase occurs in the collection
    Map<String, Integer> tf_collection = new HashMap<String, Integer>();
    Vector<String> phrase_term_v = qp.getPhraseTerm();

    // if the number of words in the qeury exceeds 5, then the smoothing value
    // is 0.7; otherwise, 0.1
    if (qp.getQuerySize() > 5) {
      smooth = 0.7;
    } else {
      smooth = 0.1;
    }

    for (String pt : phrase_term_v) {
      if (!tf_collection.containsKey(pt)) {
        tf_collection.put(pt, _indexer.corpusTermFrequency(pt));
      }
    }

    for (int i = 0; i < _indexer.numDocs(); i++) {
      DocumentIndexed doc = (DocumentIndexed) _indexer.getDoc(i);
      doc.setPageRank(_indexer.pageRankValueForDocID(doc._docid));
      doc.setNumViews(_indexer.numviewForDocID(doc._docid));
      double score = 0.0;

      for (String term : tf_collection.keySet()) {
        score += Math.log(smooth
            * (double) _indexer.documentTermFrequency(term, doc._docid)
            / (double) _indexer.getDocumentSize(i) + (1 - smooth)
            * (double) tf_collection.get(term) / (double) total_tf);
      }
      
      ScoredDocument s_d = new ScoredDocument(doc, score);
      s_d.setPageRank(_indexer.pageRankValueForDocID(doc._docid));
      s_d.setNumview(_indexer.numviewForDocID(doc._docid));
      rankQueue.add(s_d);
      if (rankQueue.size() > numResults) {
        rankQueue.poll();
      }
    }

    Vector<ScoredDocument> results = new Vector<ScoredDocument>();
    ScoredDocument scoredDoc = null;
    while ((scoredDoc = rankQueue.poll()) != null) {
      results.add(scoredDoc);
    }
    Collections.sort(results, Collections.reverseOrder());
    return results;
  }
}
