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
    "golang.org/x/net/websocket"
    "reflect"
    "runtime"
    "time"
    "math"
)

func btoi(b bool) int {
    if b {
        return 1
    }
    return 0
}

var profiling struct {
    Avg int
    ExecutionTime  time.Duration
    Executions int
    Samples int
}

var globals struct {
    ApiKey string
    httpClient http.Client
    wsExecutions *websocket.Conn
    wsQuote *websocket.Conn
}

type StockQuote struct {
    Ok bool  `json:"ok"`
    Symbol string `json:"symbol"`
    Venue string  `json:"venue"`
    Bid int  `json:"bid"`
    Ask int `json:"ask"`
    BidSize int  `json:"bidSize"`
    AskSize int `json:"askSize"`
    BidDepth int  `json:"bidDepth"`
    AskDepth int `json:"askDepth"`
    Last int `json:"last"`
    LastSize int `json:"lastSize"`
    LastTrade string  `json:"lastTrade"`
    QuoteTime string `json:"quoteTime"`
}

type StockQuoteWs struct {
    Ok bool  `json:"ok"`
    Quote struct {
        Symbol string `json:"symbol"`
        Venue string  `json:"venue"`
        Bid int  `json:"bid"`
        Ask int `json:"ask"`
        BidSize int  `json:"bidSize"`
        AskSize int `json:"askSize"`
        BidDepth int  `json:"bidDepth"`
        AskDepth int `json:"askDepth"`
        Last int `json:"last"`
        LastSize int `json:"lastSize"`
        LastTrade string  `json:"lastTrade"`
        QuoteTime string `json:"quoteTime"`
    } `json:quote`
}

var stockQuote StockQuote
var stockQuoteWs StockQuoteWs

var quoteHistory struct {
    ready bool
    history []StockQuoteWs

    lastTopBidPrice int
    lastTopAskPrice int
    avgTopBidQty float64
    avgTopAskQty float64

    avgTopBidPrice float64
    avgTopAskPrice float64

    minTopBidPrice int
    maxTopBidPrice int

    minTopAskPrice int
    maxTopAskPrice int

    lastBidPrice int
    lastAskPrice int

    lastBidId int
    lastAskId int
}

type OrderBook struct {
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

var orderBook OrderBook

var orderBookHistory struct {
    ready bool
    history []OrderBook
    lastTopBidPrice int
    lastTopAskPrice int
    avgTopBidQty float64
    avgTopAskQty float64

    avgTopBidPrice float64
    avgTopAskPrice float64

    minTopBidPrice int
    maxTopBidPrice int

    minTopAskPrice int
    maxTopAskPrice int
    quotedAsk int
    quotedBid int
}


type Position struct {
    Stock string
    Owned int
    Balance int
    NAV int
}

var data struct {
    Id string
    Venue string
    Stocks []string
    Orders map[int]Order
    Positions map[string]Position
}

type AllOrders struct {
    Ok     bool     `json:"ok"`
    Venue  string   `json:"venue"`
    Orders []Order  `json:"orders"`
}


type Order struct {
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

type Executions struct {
    Ok          bool   `json:"ok"`
    Account     string `json:"account"`
    Venue       string `json:"venue"`
    Symbol      string `json:"symbol"`
    Order Order `json:"order"`
    StandingId int `json:"standingId"`
    IncomingId int `json:"incomingId"`
    Price       int    `json:"price"`
    Filled int  `json:"filled"`
    FilledAt string `json:"filledAt"`
    StandingComplete bool `json:"standingComplete"`
    IncomingComplete bool `json:"incomingComplete"`
}

var executions Executions

func GetFunctionName(i interface{}) string {
    return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

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
        CREATE table orders (id number not null primary key, stock string, direction string, type string, price number, qty number, filled number, open integer);
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
    httpRequest, err := http.NewRequest("GET", "https://api.stockfighter.io/ob/api/heartbeat", nil)
    httpResponse, err := globals.httpClient.Do(httpRequest)

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
    //fmt.Printf("%+v\n", tempJson)
    return tempJson.Ok
}

func check_venue_inteface(venue string) bool {

    requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/heartbeat", venue)
    httpRequest, err := http.NewRequest("GET", requestUrl, nil)
    httpResponse, err := globals.httpClient.Do(httpRequest)

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

    requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/heartbeat", venue)
    httpRequest, err := http.NewRequest("GET", requestUrl, nil)
    httpResponse, err := globals.httpClient.Do(httpRequest)

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
    //fmt.Printf("%+v\n", tempJson)
    return tempJson.Ok
}

func quote_stock(venue string, stock string) bool {
    requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/stocks/%s/quote", venue, stock)
    httpRequest, err := http.NewRequest("GET", requestUrl, nil)
    httpResponse, err := globals.httpClient.Do(httpRequest)

    if err != nil {
        log.Fatal(err)
    }

    responseData, err := ioutil.ReadAll(httpResponse.Body)


    err = json.Unmarshal([]byte(responseData), &stockQuote)

    fmt.Printf("Last price: %2f\n",float32(stockQuote.Last) )

    if err != nil {
        log.Fatal(err)
    }
    return stockQuote.Ok
}

func check_stocks(venue string) bool {

    requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/stocks", venue)
    httpRequest, err := http.NewRequest("GET", requestUrl, nil)
    httpResponse, err := globals.httpClient.Do(httpRequest)

    if err != nil {
        log.Fatal(err)
    }

    responseData, err := ioutil.ReadAll(httpResponse.Body)
    //fmt.Printf("%s\n", responseData)

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
    //fmt.Printf("%+v\n", tempJson)
    return tempJson.Ok
}


func update_quotes() {
    ts := time.Now()
    MOVING_AVERAGE := 1000
    quoteHistory.avgTopBidQty = 0;
    quoteHistory.avgTopBidPrice= 0;
    quoteHistory.avgTopAskQty= 0;
    quoteHistory.avgTopAskPrice= 0;
    quoteHistory.minTopBidPrice= 100000;
    quoteHistory.maxTopBidPrice= 0;
    quoteHistory.minTopAskPrice= 100000;
    quoteHistory.maxTopAskPrice= 0;
    historyLength := len(quoteHistory.history)
    fmt.Printf("History Length: %d\n\n", historyLength)
    if historyLength > MOVING_AVERAGE {
        quoteHistory.history = quoteHistory.history[historyLength - MOVING_AVERAGE -1 : historyLength -1]
        quoteHistory.ready = true
        bidAvg:=.0
        askAvg:=.0
        for _, stockQuoteIter := range quoteHistory.history {
            quote:= stockQuoteIter.Quote
            quoteHistory.avgTopBidQty += float64(quote.BidSize)
            quoteHistory.avgTopBidPrice += float64(quote.Bid)
            quoteHistory.lastTopBidPrice = quote.Bid
            if  quote.Bid>  quoteHistory.maxTopBidPrice {
                quoteHistory.maxTopBidPrice  = quote.Bid
            }
            if  quote.Bid <  quoteHistory.minTopBidPrice {
                quoteHistory.minTopBidPrice  = quote.Bid
            }
            quoteHistory.lastBidPrice = quote.Bid;
            bidAvg+=1

            quoteHistory.avgTopAskQty += float64(quote.AskSize)
            quoteHistory.avgTopAskPrice+= float64(quote.Ask)
            quoteHistory.lastTopAskPrice = quote.Ask
            if  quote.Ask >  quoteHistory.maxTopAskPrice {
                quoteHistory.maxTopAskPrice= quote.Ask
            }
            if  quote.Ask <  quoteHistory.minTopAskPrice {
                quoteHistory.minTopAskPrice= quote.Ask
            }
            quoteHistory.lastAskPrice = quote.Ask;
            askAvg+=1
        }
        if bidAvg > 0 {
            quoteHistory.avgTopBidQty /= bidAvg;
            quoteHistory.avgTopBidPrice /= bidAvg;
        }
        if askAvg > 0 {
            quoteHistory.avgTopAskQty /= askAvg;
            quoteHistory.avgTopAskPrice /= askAvg;
        }
    }
    profiling.Samples += 1
    profiling.Executions += 1
    te:= time.Now()
    duration := te.Sub(ts)
    profiling.ExecutionTime = duration
}
func update_order_book(venue string, stock string) (bool) {

    requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/stocks/%s", venue, stock)
    httpRequest, err := http.NewRequest("GET", requestUrl, nil)
    httpResponse, err := globals.httpClient.Do(httpRequest)

    if err != nil {
        log.Fatal(err)
    }

    responseData, err := ioutil.ReadAll(httpResponse.Body)
    if err != nil {
        log.Fatal(err)
    }

    err = json.Unmarshal([]byte(responseData), &orderBook)


    orderBookHistory.history = append(orderBookHistory.history, orderBook);


    MOVING_AVERAGE := 10000
    historyLength := len(orderBookHistory.history)
    orderBookHistory.avgTopBidQty = 0;
    orderBookHistory.avgTopBidPrice= 0;
    orderBookHistory.avgTopAskQty= 0;
    orderBookHistory.avgTopAskPrice= 0;
    orderBookHistory.minTopBidPrice= 100000;
    orderBookHistory.maxTopBidPrice= 0;
    orderBookHistory.minTopAskPrice= 100000;
    orderBookHistory.maxTopAskPrice= 0;
    if historyLength > MOVING_AVERAGE {
        orderBookHistory.ready = true
        bidAvg:=.0
        askAvg:=.0
        for _, ob := range orderBookHistory.history[historyLength - MOVING_AVERAGE -1 : historyLength -1] {
            if  len(ob.Bids) > 0 {
                orderBookHistory.avgTopBidQty += float64(ob.Bids[0].Qty)
                orderBookHistory.avgTopBidPrice += float64(ob.Bids[0].Price)
                orderBookHistory.lastTopBidPrice = ob.Bids[0].Price
                if  ob.Bids[0].Price >  orderBookHistory.maxTopBidPrice {
                    orderBookHistory.maxTopBidPrice  = ob.Bids[0].Price
                }
                if  ob.Bids[0].Price <  orderBookHistory.minTopBidPrice {
                    orderBookHistory.minTopBidPrice  = ob.Bids[0].Price
                }
                bidAvg+=1
            }

            if  len(ob.Asks) > 0 {
                orderBookHistory.avgTopAskQty += float64(ob.Asks[0].Qty)
                orderBookHistory.avgTopAskPrice+= float64(ob.Asks[0].Price)
                orderBookHistory.lastTopAskPrice = ob.Asks[0].Price
                if  ob.Asks[0].Price >  orderBookHistory.maxTopAskPrice {
                    orderBookHistory.maxTopAskPrice= ob.Asks[0].Price
                }
                if  ob.Asks[0].Price <  orderBookHistory.minTopAskPrice {
                    orderBookHistory.minTopAskPrice= ob.Asks[0].Price
                }
                askAvg+=1
            }
        }
        if bidAvg > 0 {
            orderBookHistory.avgTopBidQty /= bidAvg;
            orderBookHistory.avgTopBidPrice /= bidAvg;
        }
        if askAvg > 0 {
            orderBookHistory.avgTopAskQty /= askAvg;
            orderBookHistory.avgTopAskPrice /= askAvg;
        }

    }

    //fmt.Printf("AvgTopAskPrice: %f AvgTopBidPrice :%f \n\n", orderBookHistory.avgTopAskPrice, orderBookHistory.avgTopBidPrice)


    if err != nil {
        log.Fatal(err)
    }

    if !orderBook.Ok {
        fmt.Printf("Update Order Book: %s\n\n", responseData)
    }

    return orderBook.Ok
}


func cancel_order(venue string, stock string, id int) bool {

    requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/stocks/%s/orders/%d", venue, stock, id)

    httpRequest, err := http.NewRequest("DELETE", requestUrl, nil)
    if err != nil {
        log.Fatal(err)
    }
    httpRequest.Header.Add("X-Starfighter-Authorization", globals.ApiKey)
    httpResponse, err := globals.httpClient.Do(httpRequest)

    if err != nil {
        log.Fatal(err)
    }

    responseData, err := ioutil.ReadAll(httpResponse.Body)
    //fmt.Printf("%s\n", responseData)

    var tempJson Order

    err = json.Unmarshal([]byte(responseData), &tempJson)

    if err != nil {
        log.Fatal(err)
    }

    if (tempJson.Ok) {
        check_order_status(id, venue , stock)
    }


    return tempJson.Ok
}

func place_order(venue string, stock string, direction string, account string, qty int, price int, orderType string) (bool, int, int) {


    requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/stocks/%s/orders", venue, stock)

    //POST data here
    var jsonStr = []byte(fmt.Sprintf(" { \"venue\":\"%s\",\"stock\":\"%s\",\"account\": \"%s\",\"price\":%d, \"qty\":%d,\"direction\":\"%s\", \"ordertype\":\"%s\" }", venue, stock, account, price, qty, direction, orderType))

    httpRequest, err := http.NewRequest("POST", requestUrl, bytes.NewBuffer(jsonStr))
    httpRequest.Header.Add("X-Starfighter-Authorization", globals.ApiKey)
    httpResponse, err := globals.httpClient.Do(httpRequest)

    if err != nil {
        log.Fatal(err)
    }

    responseData, err := ioutil.ReadAll(httpResponse.Body)

    var tempJson Order

    err = json.Unmarshal([]byte(responseData), &tempJson)

    if err != nil {
        log.Fatal(err)
    }

    if tempJson.Ok {
        update_order_and_position(&tempJson,nil);

    } else {
        fmt.Printf("%s\n", responseData)
    }

    /*
    if tempJson.Ok {
        sqlStmt := fmt.Sprintf(`INSERT INTO orders 
        (id, stock, direction, type, price, qty, filled, open ) 
        VALUES 
        (%d, "%s", "%s", "%s", %d, %d, %d, %d);`,
        tempJson.Id,
        tempJson.Symbol,tempJson.Direction,tempJson.OrderType,
        tempJson.Price,tempJson.Qty,tempJson.TotalFilled, btoi(tempJson.Open));

        _ , err = db.Exec (sqlStmt);

        if err != nil {
            log.Fatal(err)

        }
    }
    */
    return tempJson.Ok,tempJson.Id, tempJson.TotalFilled
}

func show_position(){
    pos := data.Positions[data.Stocks[0]]
    fmt.Printf("\nCash :%.2f Owned:%d NAV:%.2f\n", float64(pos.Balance)/100.0, pos.Owned, float64(pos.NAV)/100.0)

}

func update_position_sql(stock string, change int, price int, db *sql.DB){

    type Position struct {
        Stock string
        Owned int
        Balance int
    }

    var pos Position

    sqlStmt := fmt.Sprintf(`SELECT * FROM position WHERE stock="%s";`,stock);

    err := db.QueryRow(sqlStmt).Scan(&pos.Stock,&pos.Owned,&pos.Balance)

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

func get_all_orders(account string, venue string, stock string) bool {

    requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/accounts/%s/stocks/%s/orders", venue, account, stock)

    httpRequest, err := http.NewRequest("GET", requestUrl, nil)
    httpRequest.Header.Add("X-Starfighter-Authorization", globals.ApiKey)
    httpResponse, err := globals.httpClient.Do(httpRequest)

    if err != nil {
        fmt.Printf("%s\n", httpResponse)
        log.Fatal(err)
    }

    responseData, err := ioutil.ReadAll(httpResponse.Body)

    var tempJson AllOrders

    err = json.Unmarshal([]byte(responseData), &tempJson)

    if err != nil {
        fmt.Printf("%s\n", responseData)
        log.Fatal(err)
    }

    if tempJson.Ok {
        for _, order := range tempJson.Orders {
            update_order_and_position(&order,nil)
        }
    }

    return tempJson.Ok

}

func update_position(stock string , cashDiff int, qtyDiff int)  {
    owned:=0
    balance:=0
    if  savedPosition, ok := data.Positions[stock]; ok  {
        owned = savedPosition.Owned
        balance = savedPosition.Balance
    }

    newPosition := Position {
        Stock       :stock,
        Owned       :owned   + qtyDiff ,
        Balance     :balance + cashDiff,
        NAV         :balance + cashDiff + owned * stockQuoteWs.Quote.Last,
    }
    data.Positions[stock] = newPosition;
    //fmt.Printf("New Position %v\n", newPosition)
}

func update_order_and_position(newOrder *Order, oldOrder *Order)  {
    cashDiff:=0
    qtyDiff:=0
    if oldOrder ==  nil {
        for _ , fill := range newOrder.Fills {
            //t,_ := time.Parse(time.RFC3339Nano ,fill.Ts)
            //fmt.Printf("Adding fill to Order %d price %d qty %d, Timestamp %s \n", newOrder.Id,fill.Price,fill.Qty,t )
            cashDiff+=(fill.Price*fill.Qty)
            qtyDiff+=fill.Qty
        }
    } else  {
        tOld, _ := time.Parse(time.RFC3339Nano ,oldOrder.Ts)
        for _ , fill := range newOrder.Fills {
            tNew, _ :=  time.Parse(time.RFC3339Nano ,fill.Ts)
            if (tNew.After(tOld)) {
                fmt.Printf("Id %d price %d qty %d, Ts %s newer than %s\n", newOrder.Id,fill.Price,fill.Qty, tNew, tOld)
                cashDiff+=(fill.Price*fill.Qty)
                qtyDiff+=fill.Qty
            } else {
                //fmt.Printf("Id %d Price %d Qty %d,Ts %s older than %s\n", newOrder.Id,fill.Price,fill.Qty, tNew, tOld)
            }
        }
    }
    if newOrder.Direction == "buy" {
        update_position(newOrder.Symbol, -cashDiff, qtyDiff)
    } else {
        update_position(newOrder.Symbol, cashDiff, -qtyDiff)
    }
    //fmt.Printf("Order %d Details: %v\n", newOrder.Id, *newOrder)
    data.Orders[newOrder.Id] = *newOrder;
}

func update_executions_and_position()  {
    cashDiff:=0
    qtyDiff:=0
    order := executions.Order
    oldOrder , ok := data.Orders[order.Id]
    if !ok {
        for _ , fill := range order.Fills {
            //t,_ := time.Parse(time.RFC3339Nano ,fill.Ts)
            //fmt.Printf("Adding fill to Order %d price %d qty %d, Timestamp %s \n", order.Id,fill.Price,fill.Qty,t )
            cashDiff+=(fill.Price*fill.Qty)
            qtyDiff+=fill.Qty
        }
    } else  {
        tOld, _ := time.Parse(time.RFC3339Nano ,oldOrder.Ts)
        for _ , fill := range order.Fills {
            tNew, _ :=  time.Parse(time.RFC3339Nano ,fill.Ts)
            if (tNew.After(tOld)) {
                fmt.Printf("Id %d price %d qty %d, Ts %s newer than %s\n", order.Id,fill.Price,fill.Qty, tNew, tOld)
                cashDiff+=(fill.Price*fill.Qty)
                qtyDiff+=fill.Qty
            } else {
                //fmt.Printf("Id %d Price %d Qty %d,Ts %s older than %s\n", order.Id,fill.Price,fill.Qty, tNew, tOld)
            }
        }
    }
    if order.Direction == "buy" {
        //update_position(order.Symbol, -cashDiff, qtyDiff)
    } else {
        //update_position(order.Symbol, cashDiff, -qtyDiff)
    }
    //fmt.Printf("Order %d Details: %v\n", newOrder.Id, *order)
    data.Orders[order.Id] = order;
}

func check_order_status(id int, venue string, stock string) bool {

    requestUrl := fmt.Sprintf("https://api.stockfighter.io/ob/api/venues/%s/stocks/%s/orders/%d", venue, stock, id)

    httpRequest, err := http.NewRequest("GET", requestUrl, nil)
    httpRequest.Header.Add("X-Starfighter-Authorization", globals.ApiKey)
    httpResponse, err := globals.httpClient.Do(httpRequest)

    if err != nil {
        log.Fatal(err)
    }

    responseData, err := ioutil.ReadAll(httpResponse.Body)
    //fmt.Printf("%s\n", responseData)

    var tempJson Order

    err = json.Unmarshal([]byte(responseData), &tempJson)

    if err != nil {
        log.Fatal(err)
    }

    if tempJson.Ok {

        if  savedOrder, ok :=  data.Orders[id]; ok {
            update_order_and_position(&tempJson, &savedOrder)
        }
    }else {
        fmt.Printf("Response NOK %s\n",responseData)
    }

    return tempJson.Ok
}


func cancel_all_orders() {
    for id, order:= range data.Orders {
        if (order.Open) {
            cancel_order(data.Venue, order.Symbol, id)
        }
    }
}



func execute_strategy (strategy string) {
    switch strategy {
    case "buy":
        {
            buyQty:= 1000;
            buyPrice:= int(orderBookHistory.avgTopBidPrice)  ;
            ok, id, filled := place_order(data.Venue, data.Stocks[0], "buy", data.Id, buyQty, buyPrice, "limit")
            if (ok) {
                fmt.Printf("Buy Order sent id:%d price %d filled:%d\n", id, buyPrice, filled)

            }

            sellQty:= 900;
            sellPrice:= int(orderBookHistory.avgTopAskPrice) ;

            ok, id, filled = place_order(data.Venue, data.Stocks[0], "sell", data.Id, sellQty, sellPrice, "limit")
            if (ok) {
                fmt.Printf("Sell Order sent id:%d price %d filled:%d\n", id, sellPrice, filled)

            }
        }
    case "marketMaker":
        {
            spread := quoteHistory.lastTopAskPrice - quoteHistory.lastTopBidPrice
            buyPrice := quoteHistory.minTopAskPrice

            sellPrice := quoteHistory.maxTopBidPrice

            fmt.Printf("Buyprice :%d SellPrice :%d Last Spread :%d\n", buyPrice, sellPrice, spread)

            buyQty :=  100- data.Positions[data.Stocks[0]].Owned/2

            sellQty := 100+ data.Positions[data.Stocks[0]].Owned/2

            //if ( data.Positions[data.Stocks[0]].Owned < 0) {
            if (buyPrice < sellPrice) {
                lastBidOrder := data.Orders[quoteHistory.lastBidId]
                fmt.Printf("lastBidOrder :%d open :%t \n", lastBidOrder.Id,lastBidOrder.Open)
                if !lastBidOrder.Open {
                    if ( buyQty > 0 ){
                        ok, id, filled := place_order(data.Venue, data.Stocks[0], "buy", data.Id, buyQty, buyPrice, "limit")
                        if (ok) {
                            fmt.Printf("Buy Order sent id:%d price %d filled:%d\n", id, buyPrice, filled)
                            quoteHistory.lastBidId = id;
                        }
                    }
                } else {
                    if math.Abs(float64(lastBidOrder.Price - buyPrice)) > float64(buyPrice)*0.05 {
                        ok := cancel_order(data.Venue, lastBidOrder.Symbol, lastBidOrder.Id)
                        if ok {
                            fmt.Printf("Canceled Buy Order id:%d \n", lastBidOrder.Id)
                        }
                    }
                }
                lastAskOrder := data.Orders[quoteHistory.lastAskId]
                fmt.Printf("lastAskOrder :%d open :%t \n", lastAskOrder.Id,lastAskOrder.Open)
                if !lastAskOrder.Open {
                    if ( sellQty > 0){
                        ok, id, filled := place_order(data.Venue, data.Stocks[0], "sell", data.Id, sellQty, sellPrice, "limit")
                        if (ok) {
                            fmt.Printf("Sell Order sent id:%d price %d filled:%d\n", id, sellPrice, filled)
                            quoteHistory.lastAskId = id;
                        }
                    }
                } else {
                    if math.Abs(float64(lastAskOrder.Price - buyPrice)) > float64(sellPrice)*0.05{
                        ok:= cancel_order(data.Venue, lastAskOrder.Symbol, lastAskOrder.Id)
                        if ok {
                            fmt.Printf("Canceled Sell Order id:%d \n", lastAskOrder.Id)
                        }
                    }
                }
            }
        }
    case "level4":
        {
            //spread := quoteHistory.lastTopAskPrice - quoteHistory.lastTopBidPrice
                buyPrice := int(quoteHistory.avgTopBidPrice)
                sellPrice := int(quoteHistory.avgTopAskPrice)
            if  data.Positions[data.Stocks[0]].Owned >= -200 {
                buyPrice = int(quoteHistory.minTopAskPrice)
            }

            if  data.Positions[data.Stocks[0]].Owned <= 200 {
                sellPrice = int(quoteHistory.maxTopBidPrice)
            }
            fmt.Printf("Buyprice :%d AverageBidPrice :%d SellPrice: %d AverageAskPrice: %d\n", 
            buyPrice, int(quoteHistory.avgTopBidPrice),sellPrice,int(quoteHistory.avgTopAskPrice));

            //buyQty :=  100- data.Positions[data.Stocks[0]].Owned/2

            //sellQty := 100+ data.Positions[data.Stocks[0]].Owned/2

            buyQty :=  100 ;

                sellQty := 100  ;

            lastAskOrder := data.Orders[quoteHistory.lastAskId]
            lastBidOrder := data.Orders[quoteHistory.lastBidId]
            //if (quoteHistory.avgTopAskPrice - float64(quoteHistory.minTopAskPrice))  > quoteHistory.avgTopAskPrice*0.1 {
            if data.Positions[data.Stocks[0]].Owned < 500 && !lastBidOrder.Open {
                ok, id, filled := place_order(data.Venue, data.Stocks[0], "buy", data.Id, buyQty, buyPrice, "limit")
                if (ok) {
                    fmt.Printf("Buy Order sent id:%d price %d filled:%d\n", id, buyPrice, filled)
                    quoteHistory.lastBidId = id;
                }
            }
            if (lastBidOrder.Open) {
                tOld, _ := time.Parse(time.RFC3339Nano ,lastBidOrder.Ts)
                tNow, _ := time.Parse(time.RFC3339Nano ,stockQuoteWs.Quote.QuoteTime)
                fmt.Printf("Time Elapsed from buy order %s\n", (tNow.Sub(tOld)).String());
                if tNow.Sub(tOld) > time.Duration(20)*time.Second {
                    ok := cancel_order(data.Venue, lastBidOrder.Symbol, lastBidOrder.Id)
                    if ok {
                        fmt.Printf("Canceled Buy Order id:%d \n", lastBidOrder.Id)
                    }
                }
            }
            //if (float64(quoteHistory.maxTopBidPrice) - quoteHistory.avgTopBidPrice)  > quoteHistory.avgTopBidPrice*0.1 {
            if  sellPrice >  0 && data.Positions[data.Stocks[0]].Owned > -500 && !lastAskOrder.Open {
                ok, id, filled := place_order(data.Venue, data.Stocks[0], "sell", data.Id, sellQty, sellPrice, "limit")
                if (ok) {
                    fmt.Printf("Sell Order sent id:%d price %d filled:%d\n", id, sellPrice, filled)
                    quoteHistory.lastAskId = id;
                }
            }
            if (lastAskOrder.Open) {
                tOld, _ := time.Parse(time.RFC3339Nano ,lastAskOrder.Ts)
                tNow, _ := time.Parse(time.RFC3339Nano ,stockQuoteWs.Quote.QuoteTime)
                fmt.Printf("Time Elapsed from buy order %s\n", (tNow.Sub(tOld)).String());
                if tNow.Sub(tOld) > time.Duration(20)*time.Second {
                    ok := cancel_order(data.Venue, lastAskOrder.Symbol, lastAskOrder.Id)
                    if ok {
                        fmt.Printf("Canceled Buy Order id:%d \n", lastAskOrder.Id)
                    }
                }
            }
            // }
        }
    }
}

func init_web_sockets() {
    origin := "http://localhost/"
    urlQuote := fmt.Sprintf("wss://api.stockfighter.io/ob/api/ws/%s/venues/%s/tickertape/stocks/%s",data.Id,data.Venue,data.Stocks[0]);
    wsQuote, errWsQuote := websocket.Dial(urlQuote, "", origin)
    globals.wsQuote = wsQuote
    if errWsQuote != nil {
        log.Fatal(errWsQuote)
    }
    urlExecutions:= fmt.Sprintf("wss://api.stockfighter.io/ob/api/ws/%s/venues/%s/executions/stocks/%s",data.Id,data.Venue,data.Stocks[0]);
    wsExecutions, errWsExecutions := websocket.Dial(urlExecutions, "", origin)
    globals.wsExecutions= wsExecutions
    if errWsExecutions!= nil {
        log.Fatal(errWsExecutions)
    }
}

func update_quotes_ws() {
    for ;; {
        errWsQuote := websocket.JSON.Receive(globals.wsQuote, &stockQuoteWs)
        if errWsQuote != nil {
            log.Fatal(errWsQuote)
        }
        quoteHistory.history = append( quoteHistory.history,stockQuoteWs )
    }
}

func update_executions_ws() {
    for ;; {
        errWsExecutions := websocket.JSON.Receive(globals.wsExecutions, &executions)
        if errWsExecutions != nil {
            log.Fatal(errWsExecutions)
        }
        fmt.Printf("Received executions : %v\n", executions)
        update_executions_and_position()
    }
}

func main() {
    //Read API key from file
    PROFILING := true
    content, err := ioutil.ReadFile("./keyfile.dat")
    if err != nil {
        log.Fatal(err)
    }

    //Init globals
    globals.ApiKey = string(content);
    globals.httpClient = http.Client{}

    //Init game data
    data.Id = "FMB75081984"
    data.Venue = "EPOREX"
    data.Stocks = append(data.Stocks,"SDI")
    data.Orders = make(map[int]Order)
    data.Positions = make(map[string]Position)

    interval := 1000
    get_all_orders(data.Id,data.Venue, data.Stocks[0])

    time.Sleep(time.Duration(interval) * time.Millisecond)
    init_web_sockets()

    go update_quotes_ws()
    go update_executions_ws()

    counter:=0;

    for ;; {

        counter +=1
        fmt.Printf("Tick %d \n",counter)
        if PROFILING {
            fmt.Printf ("Execution of function %s has taken %s and it has been executed %d times\n",
            "update_quotes",
            profiling.ExecutionTime.String(),
            profiling.Executions)
        }

        data.Positions = make(map[string]Position)
        get_all_orders(data.Id,data.Venue, data.Stocks[0])

        show_position()

        //Execute strategy
        update_quotes()
        if quoteHistory.ready {
            execute_strategy("level4");
            //execute_strategy("buy");
        }
        time.Sleep(time.Duration(interval) * time.Millisecond)

    }
}
