package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func heartbeat() bool {

	httpClient := &http.Client{}

	httpRequest, err := http.NewRequest("GET", "https://api.stockfighter.io/ob/api/heartbeat", nil)
	httpResponse, err := httpClient.Do(httpRequest)

	if err != nil {
		log.Fatal(err)
	}

	responseData, err := ioutil.ReadAll(httpResponse.Body)
	httpResponse.Body.Close()

	type heartbeatResponse struct {
		Ok    bool   `json:"ok"`
		Error string `json:"error"`
	}

	var tempJson heartbeatResponse

	err = json.Unmarshal([]byte(responseData), &tempJson)

	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+v\n", tempJson)
	return tempJson.Ok
}

func check_venue_inteface(venue string) bool {

	httpClient := &http.Client{}

	requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/heartbeat", venue)
	httpRequest, err := http.NewRequest("GET", requestUrl, nil)
	httpResponse, err := httpClient.Do(httpRequest)

	if err != nil {
		log.Fatal(err)
	}

	responseData, err := ioutil.ReadAll(httpResponse.Body)
	httpResponse.Body.Close()

	jsonDecoder := json.NewDecoder(bytes.NewReader(responseData))

	var tempJson interface{}

	err = jsonDecoder.Decode(&tempJson)

	m := tempJson.(map[string]interface{})

	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d\n", m["ok"])
	return false
}

func check_venue(venue string) bool {

	httpClient := &http.Client{}

	requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/heartbeat", venue)
	httpRequest, err := http.NewRequest("GET", requestUrl, nil)
	httpResponse, err := httpClient.Do(httpRequest)

	if err != nil {
		log.Fatal(err)
	}

	responseData, err := ioutil.ReadAll(httpResponse.Body)

	type venueResponse struct {
		Ok    bool   `json:"ok"`
		Venue string `json:"venue"`
	}
	var tempJson venueResponse

	err = json.Unmarshal([]byte(responseData), &tempJson)

	if err != nil {
		log.Fatal(err)
	}
	spew.Printf("%+v\n", tempJson)
	return tempJson.Ok
}

func check_stocks(venue string) bool {

	httpClient := &http.Client{}

	requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/stocks", venue)
	httpRequest, err := http.NewRequest("GET", requestUrl, nil)
	httpResponse, err := httpClient.Do(httpRequest)

	if err != nil {
		log.Fatal(err)
	}

	responseData, err := ioutil.ReadAll(httpResponse.Body)
	fmt.Printf("%s\n", responseData)

	type stockResponse struct {
		Ok      bool `json:"ok"`
		Symbols []struct {
			Name   string `json:"name"`
			Symbol string `json:"symbol"`
		} `json:"symbols"`
	}
	var tempJson stockResponse

	err = json.Unmarshal([]byte(responseData), &tempJson)

	if err != nil {
		log.Fatal(err)
	}
	spew.Printf("%+v\n", tempJson)
	return tempJson.Ok
}

func order_book(venue string, stock string) (bool, int, int, int, int) {

	httpClient := &http.Client{}

	requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/stocks/%s", venue, stock)
	httpRequest, err := http.NewRequest("GET", requestUrl, nil)
	httpResponse, err := httpClient.Do(httpRequest)

	if err != nil {
		log.Fatal(err)
	}

	responseData, err := ioutil.ReadAll(httpResponse.Body)
	fmt.Printf("%s\n\n", responseData)

	type orderBookResponse struct {
		Ok     bool   `json:"ok"`
		Venue  string `json:"venue"`
		Symbol string `json:"symbol"`
		Bids   []struct {
			Price int  `json:"price"`
			Qty   int  `json:"qty"`
			IsBuy bool `json:"isBuy"`
		} `json:"bids"`
		Asks []struct {
			Price int  `json:"price"`
			Qty   int  `json:"qty"`
			IsBuy bool `json:"isBuy"`
		} `json:"asks"`
		Ts string `json:ts`
	}
	var tempJson orderBookResponse

	err = json.Unmarshal([]byte(responseData), &tempJson)

	if err != nil {
		log.Fatal(err)
	}
	//spew.Printf("%+v\n", tempJson)
	var topBid int
	var topBidQty int
	var topAsk int
	var topAskQty int
	topBid = 0
	topAsk = 0
	topBidQty = 0
	topAskQty = 0
	if tempJson.Ok {
		if len(tempJson.Bids) > 0 {
			topBid = tempJson.Bids[0].Price
			topBidQty = tempJson.Bids[0].Qty
		}
		if len(tempJson.Asks) > 0 {
			topAsk = tempJson.Asks[0].Price
			topAskQty = tempJson.Asks[0].Qty
		}
	}
	return tempJson.Ok, topBid, topBidQty, topAsk, topAskQty
}

func cancel_order(venue string, stock string, id int) bool {
	apiKey := "18a3819f37257c42f557e43bcfdca41ffc0ee7a6"

	httpClient := &http.Client{}

	requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/stocks/%s/orders/%d", venue, stock, id)

	httpRequest, err := http.NewRequest("DELETE", requestUrl, nil)
	if err != nil {
		log.Fatal(err)
	}
	httpRequest.Header.Add("X-Starfighter-Authorization", apiKey)
	httpResponse, err := httpClient.Do(httpRequest)

	if err != nil {
		log.Fatal(err)
	}

	responseData, err := ioutil.ReadAll(httpResponse.Body)
	fmt.Printf("%s\n", responseData)

	type cancelOrderResponse struct {
		Ok          bool   `json:"ok"`
		Symbol      string `json:"symbol"`
		Venue       string `json:"venue"`
		Direction   string `json:"direction"`
		OriginalQty int    `json:"originalQty"`
		Qty         int    `json:"qty"`
		Price       int    `json:"price"`
		OrderType   string `json:"orderType"`
		Id          int    `json:"id"`
		Account     string `json:"account"`
		Ts          string `json:ts`
		Fills       []struct {
			Price int    `json:"price"`
			Qty   int    `json:"qty"`
			Ts    string `json:ts`
		} `json:"fills"`
		TotalFilled int  `json:"totalFilled"`
		Open        bool `json:"open"`
	}
	var tempJson cancelOrderResponse

	err = json.Unmarshal([]byte(responseData), &tempJson)

	if err != nil {
		log.Fatal(err)
	}
	//spew.Printf("%+v\n", tempJson)

	return tempJson.Ok
}
func place_order(venue string, stock string, direction string, account string, qty int, price int, orderType string) (int, int) {
	apiKey := "18a3819f37257c42f557e43bcfdca41ffc0ee7a6"

	httpClient := &http.Client{}

	requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/stocks/%s/orders", venue, stock)

	//POST data here
	var jsonStr = []byte(fmt.Sprintf(" { \"venue\":\"%s\",\"stock\":\"%s\",\"account\": \"%s\",\"price\":%d, \"qty\":%d,\"direction\":\"%s\", \"ordertype\":\"%s\" }", venue, stock, account, price, qty, direction, orderType))
	//fmt.Printf(string(jsonStr))

	httpRequest, err := http.NewRequest("POST", requestUrl, bytes.NewBuffer(jsonStr))
	httpRequest.Header.Add("X-Starfighter-Authorization", apiKey)
	httpResponse, err := httpClient.Do(httpRequest)

	if err != nil {
		log.Fatal(err)
	}

	responseData, err := ioutil.ReadAll(httpResponse.Body)
	fmt.Printf("%s\n", responseData)

	type placeOrderResponse struct {
		Ok          bool   `json:"ok"`
		Symbol      string `json:"symbol"`
		Venue       string `json:"venue"`
		Direction   string `json:"direction"`
		OriginalQty int    `json:"originalQty"`
		Qty         int    `json:"qty"`
		Price       int    `json:"price"`
		OrderType   string `json:"orderType"`
		Id          int    `json:"id"`
		Account     string `json:"account"`
		Ts          string `json:ts`

		Fills []struct {
			Price int    `json:"price"`
			Qty   int    `json:"qty"`
			Ts    string `json:ts`
		} `json:"fills"`
		TotalFilled int  `json:"totalFilled"`
		Open        bool `json:"open"`
	}
	var tempJson placeOrderResponse

	err = json.Unmarshal([]byte(responseData), &tempJson)

	if err != nil {
		log.Fatal(err)
	}
	//spew.Printf("%+v\n", tempJson)
	return tempJson.Id, tempJson.TotalFilled
}

func main() {
	//heartbeat()
	//check_stocks("HMVTEX")
	var id int
	account := "BAH8735653"
	stock := "NGU"
	venue := "NWEMEX"
	var total_filled int
	total_filled = 0
	counter := 0
	bought := 0
	sold := 0
	var filled int
	filled = 0
	for total_filled < 100000 {
		ok, topBid, topBidQty, topAsk, topAskQty := order_book(venue, stock)
		fmt.Printf("Order book ok:%b topBid:%d Qty:%d topAsk:%d Qty:%d\n", ok, topBid, topBidQty, topAsk, topAskQty)
		id, filled = place_order(venue, stock, "sell", account, 1, topBid-1, "limit")
		sold = sold + filled
		fmt.Printf("Order sent id:%d filled:%d\n", id, filled)
		//fmt.Printf("Order canceled:%d\n", cancel_order(venue, stock, id))
		if (counter % 10) == 0 {
			id, filled = place_order(venue, stock, "buy", account, topAskQty, topAsk+1, "immediate-or-cancel")
			bought = bought + filled
		}

		fmt.Printf("Total Bought :%d Total sold :%d Balance %d\n", bought, sold, bought-sold)
		time.Sleep(1000 * time.Millisecond)

	}

}
