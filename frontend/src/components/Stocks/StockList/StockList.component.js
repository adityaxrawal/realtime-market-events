/*
File: Finance Project/frontend/src/components/Stocks/StockList/StockList.component.js
Description: Component to display the list of Nifty 50 stocks, handle selection,
             update with real-time data, and automatically select the first stock on load.
             (Updated with auto-selection logic)
*/
import React, { useEffect, useRef, useState, useMemo } from 'react';
// Import only HEADERS, as initial STOCK_DATA is fetched from API
import { HEADERS } from '../../../share/utils/constant';
import $ from 'jquery';
// Ensure jQuery UI components are imported
import 'jquery-ui/ui/widgets/resizable';
import 'jquery-ui/ui/widgets/selectable';
// Import component styles
import './StockList.component.scss';
// Import the child Stock component
import Stock from '../Stock/Stock.component';

// Accept onStockSelect callback and nifty50Data updates as props
const StockList = ({ onStockSelect, nifty50Data }) => {
    // Refs for DOM elements
    const headerRef = useRef(null);
    const listRef = useRef(null); // Ref for the selectable list container

    // State to hold the initial static list fetched from the API
    const [stockListData, setStockListData] = useState([]);
    // State to track the symbol of the currently selected row for UI feedback
    const [selectedSymbol, setSelectedSymbol] = useState(null);
    // State for loading indicator during initial fetch
    const [isLoading, setIsLoading] = useState(true);
    // State for error handling during initial fetch
    const [error, setError] = useState(null);
    // Ref to track if the initial auto-select has occurred
    const initialSelectDone = useRef(false);

    // --- Effect to Fetch Initial Static Stock List ---
    // Runs once when the component mounts to get the base list of stocks.
    useEffect(() => {
        const fetchInitialList = async () => {
            setIsLoading(true);
            setError(null);
            initialSelectDone.current = false; // Reset flag on fetch start
            // Use environment variable for backend URL or default to localhost
            const backendUrl = process.env.REACT_APP_API_URL || 'http://localhost:8000';
            const apiUrl = `${backendUrl}/api/v1/stocks/nifty50-list`;
            console.log(`Fetching initial stock list from: ${apiUrl}`);

            try {
                const response = await fetch(apiUrl);
                if (!response.ok) {
                    let errorDetail = `HTTP error! status: ${response.status}`;
                    try {
                        const errorData = await response.json();
                        errorDetail = errorData.detail || errorDetail;
                    } catch (parseError) { /* Ignore */ }
                    throw new Error(errorDetail);
                }
                const data = await response.json();
                // Validate received data structure
                if (data && data.stocks && Array.isArray(data.stocks)) {
                    const initialStocks = data.stocks.map(stock => ({
                        ...stock,
                        ltp: '--', // Placeholder for Last Traded Price
                        change: '--', // Placeholder for Change
                        change_percent: '--' // Placeholder for Percentage Change
                    }));
                    setStockListData(initialStocks); // Update state with fetched stocks
                    console.log(`Received ${initialStocks.length} initial stocks.`);

                    // --- Auto-select the first stock ---
                    if (initialStocks.length > 0 && typeof onStockSelect === 'function' && !initialSelectDone.current) {
                        console.log("Auto-selecting first stock:", initialStocks[0]);
                        onStockSelect(initialStocks[0]); // Call parent callback with the first stock
                        setSelectedSymbol(initialStocks[0].symbol); // Set local state for UI feedback
                        initialSelectDone.current = true; // Mark initial selection as done
                    }
                    // --- End Auto-select ---

                } else {
                    // Handle invalid data format
                    console.error("Invalid data format received for nifty50 list:", data);
                    setStockListData([]);
                    setError("Received invalid data format from server.");
                }
            } catch (err) {
                // Handle fetch errors
                console.error("Failed to fetch initial Nifty 50 list:", err);
                setStockListData([]);
                setError(err.message || "Failed to load stock list.");
            } finally {
                // Turn off loading indicator
                setIsLoading(false);
            }
        };
        fetchInitialList();
        // Disable ESLint warning for missing dependency 'onStockSelect' because
        // we intentionally want this effect to run only once on mount.
        // The auto-select logic depends on the result of this initial fetch.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []); // Empty dependency array ensures this runs only once on mount

    // --- Combine Static and Real-time Data ---
    // useMemo recalculates the displayed data only when the initial list or real-time updates change.
    const combinedStockData = useMemo(() => {
        // If initial data hasn't loaded yet, return empty array
        if (stockListData.length === 0) return [];

        // Map over the static list and merge in real-time updates from nifty50Data prop
        return stockListData.map(stock => {
            const realtimeUpdate = nifty50Data[stock.symbol]; // Find update by symbol
            // If an update exists, merge it with the static data, otherwise return the static data
            return realtimeUpdate ? { ...stock, ...realtimeUpdate } : stock;
        });
    }, [stockListData, nifty50Data]); // Dependencies: recalculate when these change

    // --- jQuery UI Effects ---
    // Effect to manage jQuery UI resizable headers and selectable list items.
    useEffect(() => {
        // --- Resizable Headers Logic ---
        const $headerCells = $(headerRef.current).find('.stock-cell');
        if ($headerCells.length > 0) {
            // Ensure existing resizable instances are destroyed before re-initializing
            if ($headerCells.data('ui-resizable')) {
                try { $headerCells.resizable('destroy'); } catch (e) { /* ignore */ }
            }

            // Initialize resizable for each header cell
            $headerCells.each(function (index) {
                const $cell = $(this);
                const initialWidth = $cell.outerWidth();

                // Apply initial width to corresponding cells in data rows
                $(`.stock-row`).each(function () {
                    $(this).children().eq(index).width(initialWidth);
                });

                // Initialize resizable interaction
                $cell.resizable({
                    handles: 'e',
                    minWidth: 80,
                    resize: function (event, ui) {
                        const newWidth = ui.size.width;
                        // Apply the new width to all corresponding cells in data rows
                        $(`.stock-menu .selectable[ref='listRef'] .stock-row`).each(function () {
                            $(this).children().eq(index).width(newWidth);
                        });
                    }
                });
            });
        }

        // --- Selectable List Logic ---
        const $list = $(listRef.current);
        if ($list.length > 0) {
            // Ensure existing selectable instance is destroyed first
            if ($list.data('ui-selectable')) {
                try { $list.selectable('destroy'); } catch (e) { /* ignore */ }
            }

            // Initialize selectable interaction on the list container
            $list.selectable({
                filter: '.stock-row',
                selected: function (event, ui) {
                    $(ui.selected).addClass('selected').siblings().removeClass('selected');
                    const selectedIndex = $(ui.selected).index();
                    const selectedStockData = combinedStockData[selectedIndex];

                    if (selectedStockData && typeof onStockSelect === 'function') {
                        onStockSelect(selectedStockData);
                        setSelectedSymbol(selectedStockData.symbol);
                        initialSelectDone.current = true; // Mark as done on manual select too
                    }
                },
                unselected: function (event, ui) {
                    $(ui.unselected).removeClass('selected');
                }
            });

            // --- Highlight the initially selected row ---
            // After selectable is initialized, find and add 'selected' class
            // to the row corresponding to the initially selected symbol.
            if (selectedSymbol) {
                const initialIndex = combinedStockData.findIndex(stock => stock.symbol === selectedSymbol);
                if (initialIndex !== -1) {
                    $list.find('.stock-row').eq(initialIndex).addClass('selected');
                }
            }
            // --- End Highlight ---

        }

        // --- Cleanup Function ---
        return () => {
            if (headerRef.current && $(headerRef.current).find('.stock-cell').data('ui-resizable')) {
                try { $(headerRef.current).find('.stock-cell').resizable('destroy'); } catch(e) {/*ignore */}
            }
            if (listRef.current && $(listRef.current).data('ui-selectable')) {
                try { $(listRef.current).selectable('destroy'); } catch(e) {/*ignore */}
            }
        };
    // Re-run effect if the combined data or the selection callback changes
    // Also added selectedSymbol to re-run highlighting if needed (though usually handled by selectable)
    }, [combinedStockData, onStockSelect, selectedSymbol]);

    // --- Render Component UI ---
    return (
        <div className="stock-menu">
            {/* Render Header Row */}
            <div className="stock-header stock-row" ref={headerRef}>
                {HEADERS.map((header, index) => (
                    <div
                        key={index}
                        className={`stock-cell header ${header.key === 'symbol' ? 'symbol' : 'stock-detail'}`}
                    >
                        {header.label}
                    </div>
                ))}
            </div>

            {/* Render Stock List Rows or Loading/Error Message */}
            {isLoading && <div className="list-message">Loading stock list...</div>}
            {error && <div className="list-message error">Error: {error}</div>}
            {!isLoading && !error && combinedStockData.length === 0 && <div className="list-message">No stocks found.</div>}

            {!isLoading && !error && combinedStockData.length > 0 && (
                // Use listRef for the selectable container
                <div className="selectable" ref={listRef}>
                    {combinedStockData.map((data, index) => (
                        <Stock
                            key={data.security_id || data.symbol || index} // Use a stable unique key
                            data={data}
                            // Pass whether this row is selected for potential styling in Stock component
                            isSelected={selectedSymbol === data.symbol}
                        />
                    ))}
                </div>
            )}
        </div>
    );
};

export default StockList;
