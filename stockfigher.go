package main

import (
	"bytes"
	"encoding/json"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func initDb(account string) *sql.DB {

    db, err := sql.Open("sqlite3", fmt.Sprintf("./%s.db",account))

    if err != nil {
        log.Fatal(err)
    }

    sqlStmt := `SELECT name FROM sqlite_master WHERE type='table' AND name='position';`

    var name string
    err = db.QueryRow(sqlStmt).Scan(&name);

    if err == sql.ErrNoRows {
        sqlStmt = `
        CREATE table position (stock string not null primary key, owned integer , balance integer) ;
        CREATE table orders (id number not null primary key, stock string, direction string, type string, price number, qty number, filled number);
        `;
        _ , err = db.Exec(sqlStmt);

        if err != nil {
            log.Fatal(err)
        } else {
            fmt.Printf("Database :%s.db created\n",account);
        }
    } else if err != nil {
        log.Print(err)
    } else {
        fmt.Printf("Database :%s.db exists\n",account);
    }

    //defer db.Close()
    return db
}

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
	fmt.Printf("%+v\n", tempJson)
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
	fmt.Printf("%+v\n", tempJson)
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

	return tempJson.Ok
}

func place_order(venue string, stock string, direction string, account string, qty int, price int, orderType string, db *sql.DB) (int, int) {
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
    if tempJson.Ok {
        sqlStmt := fmt.Sprintf(`INSERT INTO orders 
        (id, stock, direction, type, price, qty, filled) 
        VALUES 
        (%d, "%s", "%s", "%s", %d, %d, %d);`,
        tempJson.Id,
        tempJson.Symbol,tempJson.Direction,tempJson.OrderType,
        tempJson.Price,tempJson.Qty,tempJson.TotalFilled);

        _ , err = db.Exec (sqlStmt);

        if err != nil {
            log.Fatal(err)

        }
    }
    return tempJson.Id, tempJson.TotalFilled
}

func show_position(db *sql.DB){
    type Position struct {
        Stock string;
        Owned int;
        Balance int;
    }

    var pos Position

    sqlStmt := `SELECT * FROM position;`

    rows, err := db.Query(sqlStmt);

	if err == sql.ErrNoRows {
		log.Printf("No entries")
	} else if err!=nil {
		log.Fatal(err)
    }

    for rows.Next() {
        if err := rows.Scan(&pos.Stock,&pos.Owned,&pos.Balance); err!= nil {
            log.Fatal(err);
        }
        fmt.Printf ("Position in stock %s Owned:%d Balance:%d\n", pos.Stock, pos.Owned, pos.Balance);
    }

}
func update_position(stock string, change int, price int, db *sql.DB){

    type Position struct {
        Stock string;
        Owned int;
        Balance int;
    }

    var pos Position

    sqlStmt := fmt.Sprintf(`SELECT * FROM position WHERE stock="%s";`,stock);

    err := db.QueryRow(sqlStmt).Scan(&pos.Stock,&pos.Owned,&pos.Balance);;

	if err==sql.ErrNoRows{
        sqlStmt := fmt.Sprintf(`INSERT INTO position
        (stock, owned, balance) 
        VALUES 
        ("%s", %d, %d);`,stock,0,0);
        _ , err = db.Exec(sqlStmt);
    }
	if err!=nil {
		log.Fatal(err)
    }

    sqlStmt = fmt.Sprintf(`UPDATE position SET owned=%d, balance=%d  WHERE stock="%s";`,
                            pos.Owned+change, pos.Balance+price, pos.Stock);
    _ , err = db.Exec(sqlStmt);
	if err!=nil {
		log.Fatal(err)
    }
}


func main() {
	//heartbeat()
	//check_stocks("HMVTEX")
    var db *sql.DB;
	account := "MS78871452"
    db = initDb(account);

	var id int
	stock := "SDH"
	venue := "DIMBEX"
	var total_filled int
	total_filled = 0
	var filled int
    targetPrice := 9600;
	filled = 0
	for total_filled < 100000 {
		ok, topBid, topBidQty, topAsk, topAskQty := order_book(venue, stock)
		fmt.Printf("Order book ok:%b topBid:%d Qty:%d topAsk:%d Qty:%d\n", ok, topBid, topBidQty, topAsk, topAskQty)
		id, filled = place_order(venue, stock, "sell", account, 1, topBid-1, "limit",db)

        if (filled > 0) {
            update_position(stock, -filled, (topBid-1)*filled,db)
        }

		fmt.Printf("Order sent id:%d filled:%d\n", id, filled)
		//fmt.Printf("Order canceled:%d\n", cancel_order(venue, stock, id))

		if (topAsk < targetPrice){
			id, filled = place_order(venue, stock, "buy", account, topAskQty, topAsk+1, "immediate-or-cancel",db)
            if (filled > 0) {
                update_position(stock, filled, -(topAsk+1)*filled,db)
            }
		}

        show_position(db);
        time.Sleep(1000 * time.Millisecond)

	}

}
