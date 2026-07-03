package main

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type NewsItem struct {
	FeedURL         string
	Title           string
	Link            string
	Description     string
	PubDateRaw      string
	PubDate         time.Time
	ImageURL        string
	ActorHits       []string
	ControversyHits []string
}

type RSS struct {
	Channel RSSChannel `xml:"channel"`
}

type RSSChannel struct {
	Title string    `xml:"title"`
	Items []RSSItem `xml:"item"`
}

type RSSItem struct {
	Title        string         `xml:"title"`
	Link         string         `xml:"link"`
	Description  string         `xml:"description"`
	PubDateRaw   string         `xml:"pubDate"`
	GUID         string         `xml:"guid"`
	Enclosure    RSSEnclosure   `xml:"enclosure"`
	MediaContent []MediaContent `xml:"http://search.yahoo.com/mrss/ content"`
	MediaThumb   []MediaContent `xml:"http://search.yahoo.com/mrss/ thumbnail"`
}

type RSSEnclosure struct {
	URL  string `xml:"url,attr"`
	Type string `xml:"type,attr"`
}

type MediaContent struct {
	URL    string `xml:"url,attr"`
	Medium string `xml:"medium,attr"`
	Type   string `xml:"type,attr"`
}

type AtomFeed struct {
	Title   string     `xml:"title"`
	Entries []AtomItem `xml:"entry"`
}

type AtomItem struct {
	Title        string     `xml:"title"`
	Links        []AtomLink `xml:"link"`
	Summary      string     `xml:"summary"`
	Content      string     `xml:"content"`
	UpdatedRaw   string     `xml:"updated"`
	PublishedRaw string     `xml:"published"`
}

type AtomLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
}

type AggregatedRSS struct {
	XMLName xml.Name          `xml:"rss"`
	Version string            `xml:"version,attr"`
	MediaNS string            `xml:"xmlns:media,attr,omitempty"`
	Channel AggregatedChannel `xml:"channel"`
}

type AggregatedChannel struct {
	Title         string           `xml:"title"`
	Description   string           `xml:"description"`
	Link          string           `xml:"link"`
	Language      string           `xml:"language"`
	LastBuildDate string           `xml:"lastBuildDate"`
	Items         []AggregatedItem `xml:"item"`
}

type AggregatedItem struct {
	Title       string           `xml:"title"`
	Description string           `xml:"description"`
	Link        string           `xml:"link"`
	GUID        AggregatedGUID   `xml:"guid"`
	PubDate     string           `xml:"pubDate,omitempty"`
	Image       string           `xml:"image,omitempty"`
	Source      AggregatedSource `xml:"source"`
	Categories  []string         `xml:"category,omitempty"`
}

type AggregatedGUID struct {
	IsPermaLink string `xml:"isPermaLink,attr"`
	Value       string `xml:",chardata"`
}

type AggregatedSource struct {
	URL   string `xml:"url,attr"`
	Value string `xml:",chardata"`
}

func main() {
	feedFile := "rss_feeds.txt"
	outputFile := "aggregated.xml"
	politicianKeywordFile := "politiker_keywords.txt"

	if len(os.Args) > 1 {
		feedFile = os.Args[1]
	}

	if len(os.Args) > 2 {
		outputFile = os.Args[2]
	}

	feeds, err := readLines(feedFile)
	if err != nil {
		fmt.Println("Could not read feed file:", err)
		os.Exit(1)
	}

	if len(feeds) == 0 {
		fmt.Println("No feeds found in", feedFile)
		os.Exit(1)
	}

	actorKeywords := defaultActorKeywords()

	if extraPoliticians, err := readLines(politicianKeywordFile); err == nil {
		actorKeywords = append(actorKeywords, extraPoliticians...)
	}

	controversyKeywords := defaultControversyKeywords()

	client := &http.Client{
		Timeout: 25 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        500,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	itemsChannel := make(chan NewsItem, 1024)

	var wg sync.WaitGroup

	for _, feedURL := range feeds {
		wg.Add(1)

		go func(url string) {
			defer wg.Done()
			processFeed(client, url, actorKeywords, controversyKeywords, itemsChannel)
		}(feedURL)
	}

	go func() {
		wg.Wait()
		close(itemsChannel)
	}()

	var matchedItems []NewsItem
	seen := map[string]bool{}

	for item := range itemsChannel {
		key := item.Link
		if key == "" {
			key = item.Title + "|" + item.PubDateRaw
		}

		if seen[key] {
			continue
		}

		seen[key] = true
		matchedItems = append(matchedItems, item)

		fmt.Println("MATCHA:", item.Title)
		fmt.Println("Link:", item.Link)
		fmt.Println("Description:", item.Description)
		fmt.Println("Actors:", strings.Join(item.ActorHits, ", "))
		fmt.Println("Controversy:", strings.Join(item.ControversyHits, ", "))
		fmt.Println("---")
	}

	sort.SliceStable(matchedItems, func(i int, j int) bool {
		return matchedItems[i].PubDate.After(matchedItems[j].PubDate)
	})

	if err := writeAggregatedRSS(outputFile, matchedItems); err != nil {
		fmt.Println("Could not write", outputFile+":", err)
		os.Exit(1)
	}

	fmt.Println("Wrote", outputFile)
	fmt.Println("Matched items:", len(matchedItems))
}

func processFeed(client *http.Client, feedURL string, actorKeywords []string, controversyKeywords []string, out chan<- NewsItem) {
	items, err := downloadAndParseFeed(client, feedURL)
	if err != nil {
		fmt.Println("Feed failed:", feedURL+":", err)
		return
	}

	for _, item := range items {
		actorHits, controversyHits := matchNewsItem(item, actorKeywords, controversyKeywords)

		if len(actorHits) == 0 { //|| len(controversyHits) == 0
			continue
		}

		item.ActorHits = actorHits
		item.ControversyHits = controversyHits

		out <- item
	}
}

func downloadAndParseFeed(client *http.Client, feedURL string) ([]NewsItem, error) {
	body, err := downloadURLAsString(client, feedURL)
	if err != nil {
		return nil, err
	}

	items, err := parseRSS([]byte(body), feedURL)
	if err == nil && len(items) > 0 {
		return items, nil
	}

	atomItems, atomErr := parseAtom([]byte(body), feedURL)
	if atomErr == nil && len(atomItems) > 0 {
		return atomItems, nil
	}

	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("feed contained no items")
}

func downloadURLAsString(client *http.Client, rawURL string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 rss-aggregator/1.0")
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml, */*")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("HTTP status %s", resp.Status)
	}

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func parseRSS(body []byte, feedURL string) ([]NewsItem, error) {
	var rss RSS

	if err := xml.Unmarshal(body, &rss); err != nil {
		return nil, err
	}

	items := make([]NewsItem, 0, len(rss.Channel.Items))

	for _, rssItem := range rss.Channel.Items {
		link := strings.TrimSpace(rssItem.Link)
		if link == "" {
			link = strings.TrimSpace(rssItem.GUID)
		}

		items = append(items, NewsItem{
			FeedURL:     feedURL,
			Title:       cleanText(rssItem.Title),
			Link:        link,
			Description: cleanText(rssItem.Description),
			PubDateRaw:  strings.TrimSpace(rssItem.PubDateRaw),
			PubDate:     parseFeedDate(rssItem.PubDateRaw),
			ImageURL:    findImageURL(rssItem),
		})
	}

	return items, nil
}

func parseAtom(body []byte, feedURL string) ([]NewsItem, error) {
	var atom AtomFeed

	if err := xml.Unmarshal(body, &atom); err != nil {
		return nil, err
	}

	items := make([]NewsItem, 0, len(atom.Entries))

	for _, entry := range atom.Entries {
		link := ""

		for _, atomLink := range entry.Links {
			if atomLink.Rel == "" || atomLink.Rel == "alternate" {
				link = atomLink.Href
				break
			}
		}

		if link == "" && len(entry.Links) > 0 {
			link = entry.Links[0].Href
		}

		description := entry.Summary
		if description == "" {
			description = entry.Content
		}

		dateRaw := entry.PublishedRaw
		if dateRaw == "" {
			dateRaw = entry.UpdatedRaw
		}

		items = append(items, NewsItem{
			FeedURL:     feedURL,
			Title:       cleanText(entry.Title),
			Link:        strings.TrimSpace(link),
			Description: cleanText(description),
			PubDateRaw:  strings.TrimSpace(dateRaw),
			PubDate:     parseFeedDate(dateRaw),
		})
	}

	return items, nil
}

func findImageURL(item RSSItem) string {
	if item.Enclosure.URL != "" && strings.HasPrefix(strings.ToLower(item.Enclosure.Type), "image/") {
		return strings.TrimSpace(item.Enclosure.URL)
	}

	for _, media := range item.MediaContent {
		if media.URL == "" {
			continue
		}

		if media.Medium == "image" || strings.HasPrefix(strings.ToLower(media.Type), "image/") {
			return strings.TrimSpace(media.URL)
		}
	}

	for _, media := range item.MediaThumb {
		if media.URL != "" {
			return strings.TrimSpace(media.URL)
		}
	}

	return ""
}

func matchNewsItem(item NewsItem, actorKeywords []string, controversyKeywords []string) ([]string, []string) {
	haystack := strings.Join([]string{
		item.Title,
		item.Description,
		item.Link,
	}, " ")

	haystack = strings.ToLower(haystack)

	actorHits := findKeywords(haystack, actorKeywords)
	controversyHits := findKeywords(haystack, controversyKeywords)

	return actorHits, controversyHits
}

func findKeywords(haystack string, keywords []string) []string {
	var hits []string
	seen := map[string]bool{}

	for _, keyword := range keywords {
		keyword = strings.TrimSpace(keyword)
		if keyword == "" {
			continue
		}

		needle := strings.ToLower(keyword)

		if strings.Contains(haystack, needle) {
			if !seen[keyword] {
				hits = append(hits, keyword)
				seen[keyword] = true
			}
		}
	}

	return hits
}

func writeAggregatedRSS(filename string, items []NewsItem) error {
	aggregatedItems := make([]AggregatedItem, 0, len(items))

	for _, item := range items {
		description := item.Description
		if description == "" {
			description = "Matchade nyckelord: " + strings.Join(append(item.ActorHits, item.ControversyHits...), ", ")
		}

		pubDate := ""
		if !item.PubDate.IsZero() {
			pubDate = item.PubDate.Format(time.RFC1123Z)
		} else if item.PubDateRaw != "" {
			pubDate = item.PubDateRaw
		}

		categories := make([]string, 0, len(item.ActorHits)+len(item.ControversyHits))
		categories = append(categories, item.ActorHits...)
		categories = append(categories, item.ControversyHits...)

		aggregatedItems = append(aggregatedItems, AggregatedItem{
			Title:       item.Title,
			Description: description,
			Link:        item.Link,
			GUID: AggregatedGUID{
				IsPermaLink: "true",
				Value:       item.Link,
			},
			PubDate:    pubDate,
			Image:      item.ImageURL,
			Source:     AggregatedSource{URL: item.FeedURL, Value: item.FeedURL},
			Categories: categories,
		})
	}

	rss := AggregatedRSS{
		Version: "2.0",
		MediaNS: "http://search.yahoo.com/mrss/",
		Channel: AggregatedChannel{
			Title:         "Fruktpolitik",
			Description:   "Den allra senaste Fruktpolitiken",
			Link:          "https://fruktpolitik.se",
			Language:      "sv-se",
			LastBuildDate: time.Now().Format(time.RFC1123Z),
			Items:         aggregatedItems,
		},
	}

	bytes, err := xml.MarshalIndent(rss, "", "  ")
	if err != nil {
		return err
	}

	output := []byte(xml.Header + string(bytes) + "\n")

	return os.WriteFile(filename, output, 0644)
}

func readLines(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

func defaultActorKeywords() []string {
	return []string{
		"moderaterna",
		"moderaternas",
		"m-politiker",
		"nya moderaterna",
		"liberalerna",
		"liberalernas",
		"l-politiker",
		"kristdemokraterna",
		"kristdemokraternas",
		"kd-politiker",
		"sverigedemokraterna",
		"sverigedemokraternas",
		"sd-politiker",
		"m-ledamot",
		"l-ledamot",
		"kd-ledamot",
		"sd-ledamot",
		"moderat",
		"liberal",
		"kristdemokrat",
		"sverigedemokrat",
		"ulf kristersson",
		"elisabeth svantesson",
		"johan forssell",
		"maria malmer stenergard",
		"gunnar strömmer",
		"tobias billström",
		"jessika roswall",
		"lotta edholm",
		"romina pourmokhtari",
		"johan pehrson",
		"mats persson",
		"jakob forssmed",
		"ebba busch",
		"andreas carlson",
		"jimmie åkesson",
		"richard jomshof",
		"linda lindberg",
		"mattias karlsson",
		"oscar sjöstedt",
		"tobias andersson",
		"björn söder",
		"julia kronlid",
	}
}

func defaultControversyKeywords() []string {
	return []string{
		"kontrovers",
		"kontroversiell",
		"skandal",
		"kritik",
		"kritiseras",
		"kritiserar",
		"anklagas",
		"anklagelser",
		"avslöjar",
		"avslöjande",
		"granskning",
		"granskas",
		"utreds",
		"utredning",
		"polisanmälan",
		"polisanmäls",
		"åtal",
		"åtalas",
		"döms",
		"dom",
		"brott",
		"misstänks",
		"misstanke",
		"korruption",
		"jäv",
		"avgångskrav",
		"kräver avgång",
		"förtroendekris",
		"kris",
		"bråk",
		"interna strider",
		"rasism",
		"rasistisk",
		"hat",
		"hot",
		"nazism",
		"nazist",
		"antisemitism",
		"islamofobi",
		"desinformation",
		"trollkonto",
		"trollkonton",
		"kalla fakta",
		"lotteri",
		"bidragsfusk",
		"fusk",
		"felaktiga uppgifter",
		"ljugit",
		"lögn",
		"mörkat",
	}
}

func parseFeedDate(value string) time.Time {
	value = strings.TrimSpace(value)

	layouts := []string{
		time.RFC1123Z,
		time.RFC1123,
		time.RFC822Z,
		time.RFC822,
		time.RFC3339,
		"Mon, 02 Jan 2006 15:04:05 -0700",
		"Mon, 2 Jan 2006 15:04:05 -0700",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}

	for _, layout := range layouts {
		t, err := time.Parse(layout, value)
		if err == nil {
			return t
		}
	}

	return time.Time{}
}

func cleanText(value string) string {
	value = html.UnescapeString(value)
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\t", " ")
	value = strings.Join(strings.Fields(value), " ")
	return strings.TrimSpace(value)
}
