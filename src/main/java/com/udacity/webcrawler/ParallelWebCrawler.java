package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final PageParserFactory parserFactory;
  private final Duration timeout;
  private final int popularWordCount;

  private final int maxDepth;
  private final List<Pattern> ignoredUrls;
  private final ForkJoinPool pool;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          PageParserFactory parserFactory,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @MaxDepth int maxDepth,
          @IgnoredUrls List<Pattern> ignoredUrls,
          @TargetParallelism int threadCount) {
    this.clock = clock;
    this.parserFactory=parserFactory;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.maxDepth=maxDepth;
    this.ignoredUrls=ignoredUrls;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    Map<String, Integer> counts = new ConcurrentHashMap<>();
    Set<String> visitedUrls = Collections.newSetFromMap(new ConcurrentHashMap<>());
    //Set<String> visitedUrls = Collections.newSetFromMap(counts);

    ForkJoinPool pool = new ForkJoinPool();

    List<CrawlTask> tasks = new ArrayList<>();
    for (String url : startingUrls) {
      CrawlTask task = new CrawlTask(clock,url, deadline, maxDepth, parserFactory, ignoredUrls, counts, visitedUrls);
      tasks.add(task);
      pool.execute(task);
    }

    pool.shutdown();
    try {
      pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }

    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();

    //return new CrawlResult.Builder().build();
  }

  private static class CrawlTask extends RecursiveAction {
    private final Clock clock;
    private final String url;
    private final Instant deadline;
    private final int maxDepth;
    private final PageParserFactory parserFactory;
    private final List<Pattern> ignoredUrls;
    private final Map<String, Integer> counts;
    private final Set<String> visitedUrls;

    public CrawlTask(Clock clock,String url, Instant deadline, int maxDepth, PageParserFactory parserFactory,
                     List<Pattern> ignoredUrls, Map<String, Integer> counts, Set<String> visitedUrls) {
      this.clock=clock;
      this.url = url;
      this.deadline = deadline;
      this.maxDepth = maxDepth;
      this.parserFactory = parserFactory;
      this.ignoredUrls = ignoredUrls;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
    }

    @Override
    protected void compute() {
      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return;
      }
      for (Pattern pattern : ignoredUrls) {
        if (pattern.matcher(url).matches()) {
          return;
        }
      }
      if(!visitedUrls.add(url)) {
        return;
      }
      PageParser.Result result = parserFactory.get(url).parse();
      for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        counts.merge(e.getKey(), e.getValue(), Integer::sum);
      }
      List<CrawlTask> subtasks = new ArrayList<>();
      for (String link : result.getLinks()) {
        CrawlTask task = new CrawlTask(clock,link, deadline, maxDepth - 1, parserFactory, ignoredUrls, counts, visitedUrls);
        subtasks.add(task);
      }
      invokeAll(subtasks);
    }
  }





  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}