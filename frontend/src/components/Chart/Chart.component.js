/*
File: Finance Project/frontend/src/components/Chart/Chart.component.js
Description: Component responsible for rendering the stock chart using Lightweight Charts,
             fetching historical data, and displaying real-time updates.
             (Updated with refined initialization and logging)
*/
import React, { useEffect, useRef, useState } from 'react';
import { createChart, CrosshairMode } from 'lightweight-charts';
import './Chart.component.css'; // Import the associated CSS

const Chart = ({ selectedStock, quoteData }) => {
    // Ref for the container div where the chart will be mounted
    const chartContainerRef = useRef(null);
    // Ref to hold the Lightweight Charts chart instance
    const chartRef = useRef(null);
    // Ref to hold the main candlestick series instance
    const candlestickSeriesRef = useRef(null);
    // State for loading indicator
    const [isLoading, setIsLoading] = useState(false);
    // State for displaying errors
    const [error, setError] = useState(null);

    // --- Function to Fetch Historical Data ---
    // (This function remains the same as the previous version)
    const fetchHistoricalData = async (stock) => {
        // Guard clause: Ensure necessary stock details are present
        if (!stock || !stock.security_id || !stock.exchange || !stock.segment) {
            console.warn("Cannot fetch historical data: Required stock details missing.", stock);
            // Clear the chart if stock details are invalid or missing
            if (candlestickSeriesRef.current) {
                candlestickSeriesRef.current.setData([]);
            }
            setError("Invalid stock selected."); // Set an error message
            setIsLoading(false);
            return;
        }

        console.log(`Fetching historical data for ${stock.symbol} (ID: ${stock.security_id})...`);
        setIsLoading(true);
        setError(null); // Clear previous errors

        // --- Construct API URL ---
        // Use environment variable for backend URL or default to localhost
        const backendUrl = process.env.REACT_APP_API_URL || 'http://localhost:8000';
        // Define date range (e.g., last 365 days)
        const toDate = new Date();
        const fromDate = new Date();
        fromDate.setDate(toDate.getDate() - 365); // Go back 365 days

        // Prepare query parameters for the API request
        const params = new URLSearchParams({
            exchange_segment: `${stock.exchange}_${stock.segment.toUpperCase()}`, // Format: NSE_EQUITY
            instrument_type: stock.instrument_type || "EQUITY", // Default if not provided
            from_date: fromDate.toISOString().split('T')[0], // Format: YYYY-MM-DD
            to_date: toDate.toISOString().split('T')[0], // Format: YYYY-MM-DD
            interval: 'D' // Request Daily interval data
        });

        // Construct the full API endpoint URL
        const apiUrl = `${backendUrl}/api/v1/stocks/historical/${stock.security_id}?${params.toString()}`;
        console.log(`Requesting Historical Data from: ${apiUrl}`);

        try {
            // Fetch data from the backend API
            const response = await fetch(apiUrl);
            if (!response.ok) {
                // Try to parse error detail from backend response
                let errorDetail = `HTTP error! status: ${response.status}`;
                try {
                    const errorData = await response.json();
                    errorDetail = errorData.detail || errorDetail;
                } catch (parseError) {
                    // Ignore if response body is not JSON
                }
                throw new Error(errorDetail);
            }
            const data = await response.json();

            // Validate the received data structure
            if (data && data.data && Array.isArray(data.data)) {
                // Format the data points for Lightweight Charts library
                // Requires { time: epochSeconds, open, high, low, close }
                const formattedData = data.data.map(item => ({
                    time: item.timestamp, // Assuming backend provides epoch seconds
                    open: item.open,
                    high: item.high,
                    low: item.low,
                    close: item.close
                })).sort((a, b) => a.time - b.time); // Ensure data is sorted by time ascending

                console.log(`Received and formatted ${formattedData.length} historical data points.`);

                // Update the chart series with the fetched data
                if (candlestickSeriesRef.current) {
                    candlestickSeriesRef.current.setData(formattedData);
                    // Adjust the visible time range to fit the loaded data
                    chartRef.current?.timeScale().fitContent();
                } else {
                    console.warn("Candlestick series ref is not available when trying to set data.");
                }
            } else {
                // Handle cases where data is empty or malformed
                console.warn("Historical data received is empty or in invalid format:", data);
                if (candlestickSeriesRef.current) {
                    candlestickSeriesRef.current.setData([]); // Clear the chart
                }
                setError("Received invalid data format from server.");
            }
        } catch (err) {
            // Handle fetch errors (network issues, API errors)
            console.error("Failed to fetch historical data:", err);
            setError(err.message || "Failed to load chart data.");
            // Clear the chart on error
            if (candlestickSeriesRef.current) {
                candlestickSeriesRef.current.setData([]);
            }
        } finally {
            // Ensure loading state is turned off
            setIsLoading(false);
        }
    };

    // --- Chart Initialization and Cleanup Effect ---
    // Runs once when the component mounts.
    useEffect(() => {
        // Ensure the container element is available
        if (!chartContainerRef.current) {
            console.log("Chart container ref not available yet.");
            return;
        }
        console.log("Initializing chart...");

        // Define chart appearance and behavior options
        const chartOptions = {
            layout: {
                background: { color: '#000000' },
                textColor: '#C3C5CB',
            },
            grid: {
                vertLines: { color: '#2B2B43' },
                horzLines: { color: '#2B2B43' },
            },
            crosshair: {
                mode: CrosshairMode.Normal,
            },
            rightPriceScale: {
                borderColor: '#485c7b',
            },
            timeScale: {
                borderColor: '#485c7b',
                timeVisible: true,
                secondsVisible: false,
            },
            // Optional: Add a watermark
            // watermark: { ... },
        };

        // --- Refined Initialization ---
        let chart = null; // Local variable for the chart instance
        try {
            // Create the chart instance
            chart = createChart(chartContainerRef.current, chartOptions);
            chartRef.current = chart; // Assign to the ref
            console.log("Chart instance created:", chartRef.current);

            // Check if the instance is valid and has the method BEFORE calling it
            if (chartRef.current && typeof chartRef.current.addCandlestickSeries === 'function') {
                // Add the candlestick series
                candlestickSeriesRef.current = chartRef.current.addCandlestickSeries({
                    upColor: '#26a69a',
                    downColor: '#ef5350',
                    borderVisible: false,
                    wickUpColor: '#26a69a',
                    wickDownColor: '#ef5350',
                });
                console.log("Candlestick series added.");
            } else {
                // Log an error if the chart instance seems invalid right after creation
                console.error("CRITICAL: chartRef.current is invalid or addCandlestickSeries is not a function immediately after createChart.", chartRef.current);
                setError("Failed to initialize chart series.");
                // If chart creation failed partially, attempt cleanup if possible
                if (chartRef.current && typeof chartRef.current.remove === 'function') {
                    chartRef.current.remove();
                    chartRef.current = null;
                }
                return; // Stop further execution in this effect
            }
        } catch (initError) {
            // Catch any errors during createChart or addCandlestickSeries
            console.error("Error during chart/series initialization:", initError);
            setError("Failed to initialize chart instance.");
            // Ensure refs are nullified on error
            chartRef.current = null;
            candlestickSeriesRef.current = null;
            return; // Stop further execution
        }
        // --- End Refined Initialization ---


        // --- Resize Handling ---
        const resizeObserver = new ResizeObserver(entries => {
            if (entries.length > 0 && entries[0].contentRect) {
                const { width, height } = entries[0].contentRect;
                // Check if chart instance exists before resizing
                if (chartRef.current) {
                    chartRef.current.resize(width, height);
                }
            }
        });
        if (chartContainerRef.current) {
            resizeObserver.observe(chartContainerRef.current);
        }

        // --- Cleanup Function ---
        return () => {
            console.log("Cleaning up chart...");
            if (chartContainerRef.current) {
                resizeObserver.unobserve(chartContainerRef.current); // Use unobserve
            }
            resizeObserver.disconnect();
            // Check if chart instance exists before removing
            if (chartRef.current) {
                chartRef.current.remove();
                console.log("Chart instance removed.");
            }
            // Nullify refs on cleanup
            chartRef.current = null;
            candlestickSeriesRef.current = null;
        };
    }, []); // Empty dependency array ensures this effect runs only once

    // --- Effect to Fetch Data on Stock Change ---
    // (This effect remains the same as the previous version)
    useEffect(() => {
        if (selectedStock && candlestickSeriesRef.current) {
            fetchHistoricalData(selectedStock);
            if (chartRef.current) {
                chartRef.current.applyOptions({
                    watermark: {
                        visible: true,
                        text: selectedStock.symbol,
                        fontSize: 36,
                        color: 'rgba(180, 180, 180, 0.2)',
                        horzAlign: 'center',
                        vertAlign: 'center',
                    },
                });
            }
        } else if (!selectedStock && candlestickSeriesRef.current) {
            candlestickSeriesRef.current.setData([]);
            if (chartRef.current) {
                chartRef.current.applyOptions({ watermark: { visible: false } });
            }
            setError(null);
            setIsLoading(false);
        }
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedStock]);

    // --- Effect to Handle Real-time Quote Updates ---
    // (This effect remains the same as the previous version)
    useEffect(() => {
        if (quoteData && candlestickSeriesRef.current && selectedStock && quoteData.symbol === selectedStock.symbol) {
            if (quoteData.ltt && quoteData.open && quoteData.high && quoteData.low && quoteData.ltp) {
                candlestickSeriesRef.current.update({
                    time: quoteData.ltt,
                    open: quoteData.open,
                    high: quoteData.high,
                    low: quoteData.low,
                    close: quoteData.ltp
                });
            }
        }
    }, [quoteData, selectedStock]);

    // --- Render Chart Container and Messages ---
    // (This section remains the same as the previous version)
    return (
        <div className="chart-container" ref={chartContainerRef}>
            {isLoading && <div className="chart-message loading">Loading chart data...</div>}
            {error && <div className="chart-message error">Error: {error}</div>}
            {!selectedStock && !isLoading && !error && (
                <div className="chart-message placeholder">Select a stock to view the chart</div>
            )}
        </div>
    );
};

export default Chart;
