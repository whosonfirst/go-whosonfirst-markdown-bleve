package bleve

import (
	blv "github.com/blevesearch/bleve"
	"github.com/whosonfirst/go-whosonfirst-markdown"
	"github.com/whosonfirst/go-whosonfirst-markdown/search"
	_ "log"
	"os"
)

type BleveIndexer struct {
	search.Indexer
	index blv.Index
}

func NewBleveIndexer(path string) (search.Indexer, error) {

	var idx blv.Index

	_, err := os.Stat(path)

	if os.IsNotExist(err) {

		m := blv.NewIndexMapping()
		b, err := blv.New(path, m)

		if err != nil {
			return nil, err
		}

		idx = b
	} else {

		b, err := blv.Open(path)

		if err != nil {
			return nil, err
		}

		idx = b
	}

	i := BleveIndexer{
		index: idx,
	}

	return &i, err
}

func (i *BleveIndexer) Close() error {
	return nil
}

func (i *BleveIndexer) Query(q *search.SearchQuery) (interface{}, error) {
	query := blv.NewQueryStringQuery(q.QueryString)
	req := blv.NewSearchRequest(query)
	return i.index.Search(req)
}

func (i *BleveIndexer) IndexDocument(doc *markdown.Document) (*search.SearchDocument, error) {

	search_doc, err := search.NewSearchDocument(doc)

	if err != nil {
		return nil, err
	}

	err = i.index.Index(search_doc.Id, search_doc)

	if err != nil {
		return nil, err
	}

	return search_doc, nil
}
