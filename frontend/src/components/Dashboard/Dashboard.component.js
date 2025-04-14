/*
File: Finance Project/frontend/src/components/Dashboard/Dashboard.component.js
Description: Main dashboard component managing layout, WebSocket connection,
             and state for selected stock and real-time data updates.
*/
import React, { useEffect, useRef, useState, useCallback } from 'react';
import Sentiment from '../Sentiment/Sentiment.component';
import Chart from '../Chart/Chart.component';
import StockList from '../Stocks/StockList/StockList.component';
import $ from 'jquery';
// Ensure jQuery UI components are imported correctly
import 'jquery-ui/ui/widgets/resizable';
// Import the associated CSS
import './Dashboard.component.css';

const Dashboard = () => {
    // Refs for DOM elements
    const containerRef = useRef(null);
    const leftRef = useRef(null);
    const rightRef = useRef(null);
    // Ref for the WebSocket connection
    const socket = useRef(null);

    // --- State Management ---
    // State for the currently selected stock details
    const [selectedStock, setSelectedStock] = useState(null); // Example: { symbol: 'RELIANCE', security_id: '2885', exchange: 'NSE', segment: 'EQUITY', ... }
    // State to hold real-time ticker data for the Nifty 50 list
    const [nifty50Data, setNifty50Data] = useState({}); // Example: { "RELIANCE": { ltp: 2985.50, change: 10.20, ... }, ... }
    // State to hold the latest detailed quote data for the selected stock
    const [quoteData, setQuoteData] = useState(null);

    // --- Callback for Stock Selection ---
    // Memoized callback passed to StockList to handle stock selection events.
    const handleStockSelect = useCallback((stock) => {
        console.log("Stock selected in Dashboard:", stock);

        // Avoid unnecessary unsubscribe/subscribe if the same stock is clicked again
        if (selectedStock && selectedStock.security_id === stock.security_id) {
            setSelectedStock(stock); // Update state anyway in case other details changed
            return;
        }

        // Unsubscribe from the previously selected stock's quote feed via WebSocket
        if (selectedStock && socket.current && socket.current.readyState === WebSocket.OPEN) {
            const unsubscribeMsg = {
                action: "unsubscribe_ohlc", // Action defined by backend WebSocket protocol
                params: {
                    security_id: selectedStock.security_id,
                    // Construct the exchange_segment string (e.g., "NSE_EQUITY")
                    exchange_segment: selectedStock.segment ? `${selectedStock.exchange}_${selectedStock.segment.toUpperCase()}` : "NSE_EQ"
                }
            };
            console.log("Sending unsubscribe:", unsubscribeMsg);
            socket.current.send(JSON.stringify(unsubscribeMsg));
            setQuoteData(null); // Clear previous quote data immediately
        }

        // Update the state with the newly selected stock
        setSelectedStock(stock);

        // Subscribe to the newly selected stock's quote feed via WebSocket
        if (stock && socket.current && socket.current.readyState === WebSocket.OPEN) {
            const subscribeMsg = {
                action: "subscribe_ohlc", // Action defined by backend WebSocket protocol
                params: {
                    security_id: stock.security_id,
                    exchange_segment: stock.segment ? `${stock.exchange}_${stock.segment.toUpperCase()}` : "NSE_EQ"
                }
            };
            console.log("Sending subscribe:", subscribeMsg);
            socket.current.send(JSON.stringify(subscribeMsg));
        }
    }, [selectedStock]); // Depends on the currently selected stock

    // --- WebSocket Message Handling ---
    // Memoized callback to process messages received from the backend WebSocket.
    const handleWebSocketMessage = useCallback((event) => {
        try {
            const message = JSON.parse(event.data);

            // Process 'ticker_update' messages for the Nifty 50 list
            if (message.type === 'ticker_update' && message.payload) {
                setNifty50Data(prevData => ({
                    ...prevData,
                    [message.payload.symbol]: { // Use symbol as the key
                        ltp: message.payload.ltp,
                        change: message.payload.change,
                        change_percent: message.payload.percent_change,
                        // Include any other relevant fields provided in the ticker payload
                    }
                }));
            // Process 'quote_update' messages for the currently selected stock
            } else if (message.type === 'quote_update' && message.payload) {
                // Only update if the quote belongs to the selected stock
                if (selectedStock && message.payload.symbol === selectedStock.symbol) {
                    setQuoteData(message.payload); // Update the state with the new quote data
                }
            // Log confirmation, error, or informational messages from the WebSocket server
            } else if (message.type === 'subscribed' || message.type === 'unsubscribed' || message.type === 'error') {
                console.log("WS Info/Error:", message);
            } else if (typeof message === 'string' && message.startsWith("Welcome")) {
                console.log("WS Server Message:", message);
            }
            // Optionally log unhandled message types for debugging
            // else { console.log("Received unhandled WS message type:", message.type); }

        } catch (error) {
            // Log errors during message parsing
            console.error('Failed to parse WebSocket message:', event.data, error);
        }
    }, [selectedStock]); // Depends on selectedStock to correctly route quote updates

    // --- WebSocket Connection Effect ---
    // Effect to establish and manage the WebSocket connection lifecycle.
    useEffect(() => {
        // Retrieve WebSocket URL from environment variables or use a default
        const wsUrl = process.env.REACT_APP_WEBSOCKET_URL || 'ws://localhost:8000/ws/stocks';
        console.log(`Attempting to connect to WebSocket at ${wsUrl}`);
        socket.current = new WebSocket(wsUrl);

        // Assign event handlers
        socket.current.onopen = () => console.log('WebSocket Connected');
        socket.current.onclose = () => console.log('WebSocket Disconnected');
        socket.current.onerror = (error) => console.error('WebSocket Error:', error);
        socket.current.onmessage = handleWebSocketMessage; // Use the memoized handler

        // Cleanup function to close the WebSocket connection when the component unmounts
        return () => {
            if (socket.current) {
                console.log("Closing WebSocket connection.");
                socket.current.close();
            }
        };
        // Re-run this effect if the message handler changes (due to selectedStock dependency)
        // This ensures the handler always has the latest selectedStock reference.
    }, [handleWebSocketMessage]);

    // --- jQuery UI Resizable Effect ---
    // Effect to initialize the jQuery UI resizable handle between left and right panels.
    useEffect(() => {
        const $left = $(leftRef.current);
        const $right = $(rightRef.current);
        const $container = $(containerRef.current);

        // Ensure all required DOM elements are available
        if (!$left.length || !$right.length || !$container.length) {
            console.warn("Resizable panels not found, skipping initialization.");
            return;
        }

        const totalWidth = $container.width();

        // Set initial panel widths based on a percentage (e.g., 70/30 split)
        const initialLeftWidth = totalWidth * 0.7;
        $left.width(initialLeftWidth);
        $right.width(totalWidth - initialLeftWidth);

        // Initialize the resizable interaction on the left panel
        $left.resizable({
            handles: 'e', // Enable resizing only from the right edge ('east')
            minWidth: 200, // Minimum width for the left panel
            maxWidth: totalWidth - 200, // Maximum width, leaving space for the right panel
            resize: function (event, ui) {
                // Adjust the width of the right panel dynamically during resize
                const newLeftWidth = ui.size.width;
                const newRightWidth = totalWidth - newLeftWidth;
                $right.width(newRightWidth);
                // Dispatch a window resize event to allow nested components (like charts) to adjust
                window.dispatchEvent(new Event('resize'));
            }
        });

        // Cleanup function to destroy the resizable instance when the component unmounts
        return () => {
            // Check if the resizable widget was initialized before attempting to destroy it
            if ($left.data('ui-resizable')) {
                try {
                    $left.resizable('destroy');
                } catch (error) {
                    console.error("Error destroying jQuery UI resizable:", error);
                }
            }
        };
    }, []); // Empty dependency array ensures this effect runs only once on mount

    // --- Render Dashboard Layout ---
    return (
        <div className='dashboard-container' ref={containerRef}>
            {/* Left Panel: Chart */}
            <div className='dashboard-left' ref={leftRef}>
                <Chart selectedStock={selectedStock} quoteData={quoteData} />
            </div>
            {/* Right Panel: Stock List and Sentiment */}
            <div className='dashboard-right' ref={rightRef}>
                {/* Top Right: Stock List */}
                <div className='dashboard-right-top'>
                    <StockList onStockSelect={handleStockSelect} nifty50Data={nifty50Data} />
                </div>
                {/* Bottom Right: Sentiment Analysis */}
                <div className='dashboard-right-bottom'>
                    <Sentiment selectedStock={selectedStock} />
                </div>
            </div>
        </div>
    );
};

export default Dashboard;
