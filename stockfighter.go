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

func btoi(b bool) int {
    if b {
        return 1
    }
    return 0
}

var globals struct {
    ApiKey string
    httpClient http.Client
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

var orderBook OrderBook;

var orderBookHistory struct {
    ready bool
    history []OrderBook;
    avgTopBidQty float64;
    avgTopAskQty float64;

    avgTopBidPrice float64;
    avgTopAskPrice float64;

    minTopBidPrice int;
    maxTopBidPrice int;

    minTopAskPrice int;
    maxTopAskPrice int;
}


type Position struct {
    Stock string
    Owned int
    Balance int
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

    MOVING_AVERAGE := 10
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

    fmt.Printf("AvgTopAskPrice: %f AvgTopBidPrice :%f \n\n", orderBookHistory.avgTopAskPrice, orderBookHistory.avgTopBidPrice)


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

func update_position(stock string , qtyDiff int , cashDiff int)  {
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
    }

    data.Positions[stock] = newPosition;
    fmt.Printf("New Position %v\n", newPosition)

}

func update_order_and_position(newOrder *Order, oldOrder *Order)  {
    cashDiff:=0
    qtyDiff:=0
    if oldOrder ==  nil {
        for _ , fill := range newOrder.Fills {
            t,_ := time.Parse(time.RFC3339Nano ,fill.Ts)
            fmt.Printf("Adding fill to Order %d price %d qty %d, Timestamp %s \n", newOrder.Id,fill.Price,fill.Qty,t )
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
    if newOrder.Direction == "buy" { update_position(newOrder.Symbol, -cashDiff, qtyDiff)
} else {
    update_position(newOrder.Symbol, cashDiff, -qtyDiff)
}
//fmt.Printf("Order %d Details: %v\n", newOrder.Id, *newOrder)
data.Orders[newOrder.Id] = *newOrder;
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
            if tempJson.TotalFilled != savedOrder.TotalFilled {

                update_order_and_position(&tempJson, &savedOrder)

            }
        }
    }else {
        fmt.Printf("Response NOK %s\n",responseData)
    }


    /*
    if tempJson.Ok {
        sqlStmt := fmt.Sprintf(`UPDATE orders SET 
        "filled"=%d;`,
        id,tempJson.TotalFilled);
        _ , err = db.Exec (sqlStmt);

        if err != nil {
            log.Fatal(err)

        }
    }
    */

    return tempJson.Ok
}

func review_orders() {
    for id, order:= range data.Orders {
        if (order.Open) {
            //fmt.Printf("Checking order %d %v \n",id,order)
            check_order_status(id, data.Venue, order.Symbol)
        }
    }
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
            buyPrice:= int(orderBookHistory.avgTopAskPrice)  ;
            buyQty:= 1000;
            ok, id, filled := place_order(data.Venue, data.Stocks[0], "buy", data.Id, buyQty, buyPrice, "limit")
            if (ok) {
                fmt.Printf("Buy Order sent id:%d price %d filled:%d\n", id, buyPrice, filled)

            }

            sellPrice:= int(orderBookHistory.avgTopBidPrice) ;
            sellQty:= 900;

            ok, id, filled = place_order(data.Venue, data.Stocks[0], "sell", data.Id, sellQty, sellPrice, "limit")
            if (ok) {
                fmt.Printf("Sell Order sent id:%d price %d filled:%d\n", id, sellPrice, filled)

            }
        }
    case "marketMaker":
        {
            buyPrice:= int(orderBookHistory.maxTopAskPrice)
            buyQty:= 1000
            sellPrice:= int(orderBookHistory.maxTopBidPrice)
            sellQty:= 1000

            maxInventory := 10000
            //Avoid building excessive inventory
            if postion.Owned < maxInventory {
                //Avoid self trades
                if buyPrice < sellPrice {
                    ok, id, filled := place_order(data.Venue, data.Stocks[0], "buy", data.Id, buyQty, buyPrice, "immediate-or-cancel")
                    if (ok) {
                        fmt.Printf("Buy Order sent id:%d price %d filled:%d\n", id, buyPrice, filled)

                    }
                }
            }

            //Avoid building excessive inventory
            if postion.Owned > -maxInventory {
                //Avoid self trades
                if buyPrice < sellPrice {
                    ok, id, filled = place_order(data.Venue, data.Stocks[0], "sell", data.Id, sellQty, sellPrice, "immediate-or-cancel")
                    if (ok) {
                        fmt.Printf("Sell Order sent id:%d price %d filled:%d\n", id, sellPrice, filled)

                    }
                }
            }


        }
    }
}


func main() {
    //Read API key from file
    content, err := ioutil.ReadFile("./keyfile.dat")

    //Init globals
    globals.ApiKey = string(content);
    globals.httpClient = http.Client{}

    //Init game data
    data.Id = "HKS34098810"
    data.Venue = "EZDLEX"
    data.Stocks = append(data.Stocks,"MUED")
    data.Orders = make(map[int]Order)
    data.Positions = make(map[string]Position)

    fmt.Printf("\nStart Main Loop\n")

    //Sync position and order status from server
    for _, stock := range data.Stocks {
        get_all_orders(data.Id,data.Venue, stock)
    }

    counter:=0;

    interval := 2000
    for ;; {
        counter +=1
        fmt.Printf("Tick %d \n",counter)
        //Check for new fills to update position
        review_orders()

        //Update order book
        update_order_book(data.Venue, data.Stocks[0])

        //Execute strategy
        if orderBookHistory.ready {
            execute_strategy("marketMaker");
            //execute_strategy("buy");
        }
        time.Sleep(time.Duration(interval) * time.Millisecond)
    }
}
