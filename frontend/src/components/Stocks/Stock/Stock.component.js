/*
File: Finance Project/frontend/src/components/Stocks/Stock/Stock.component.js
Description: Component to render a single row in the stock list, displaying
             stock name, price, and change information with appropriate formatting and coloring.
*/
import React from 'react';

/**
 * Determines the color based on the change value.
 * Green for positive, Red for negative, Default Grey otherwise.
 * Handles non-numeric inputs gracefully.
 * @param {*} value - The change value (number or string like '--')
 * @returns {string} - CSS color string
 */
const getChangeColor = (value) => {
    const numericValue = parseFloat(value);
    if (isNaN(numericValue)) {
        return '#DBDBDB'; // Default color for non-numeric placeholders
    }
    return numericValue > 0 ? '#26a69a' : numericValue < 0 ? '#ef5350' : '#DBDBDB';
};

/**
 * Formats a numeric value to two decimal places.
 * Handles non-numeric inputs gracefully.
 * @param {*} value - The value to format (number or string like '--')
 * @returns {string} - Formatted string or original value
 */
const formatValue = (value) => {
    if (typeof value === 'number') {
        return value.toFixed(2);
    }
    // Check if it's a string representation of a number before parsing
    if (typeof value === 'string' && !isNaN(parseFloat(value))) {
        return parseFloat(value).toFixed(2);
    }
    return value; // Return placeholder ('--') or other non-numeric value as is
};

/**
 * Formats the change value, adding a '+' sign for positive numbers.
 * Handles non-numeric inputs gracefully.
 * @param {*} value - The change value (number or string like '--')
 * @returns {string} - Formatted string or original value
 */
const formatChange = (value) => {
    const numericValue = parseFloat(value);
    if (isNaN(numericValue)) {
        return value; // Return placeholder as is
    }
    return numericValue > 0 ? `+${numericValue.toFixed(2)}` : numericValue.toFixed(2);
};

/**
 * Formats the percentage change value, adding a '+' sign and '%' suffix.
 * Handles non-numeric inputs gracefully.
 * @param {*} value - The percentage change value (number or string like '--')
 * @returns {string} - Formatted string or original value
 */
const formatChangePercent = (value) => {
    const numericValue = parseFloat(value);
    if (isNaN(numericValue)) {
        return value; // Return placeholder as is
    }
    return numericValue > 0 ? `+${numericValue.toFixed(2)}%` : `${numericValue.toFixed(2)}%`;
};

/**
 * Stock Component: Renders a single row in the stock list.
 * @param {object} props - Component props
 * @param {object} props.data - Stock data object containing symbol, name, ltp, change, change_percent etc.
 * @param {boolean} props.isSelected - Flag indicating if this row is currently selected.
 */
const Stock = ({ data, isSelected }) => {
    // Define the CSS class for the row, adding 'actively-selected' if the isSelected prop is true.
    // This allows for optional specific styling beyond the jQuery UI selectable highlight.
    const rowClass = `stock-row ${isSelected ? 'actively-selected' : ''}`;

    return (
        <div className={rowClass}>
            {/* Column 1: Stock Name/Symbol */}
            <div className="stock-cell symbol" title={data.symbol}>
                {/* Display company name if available, otherwise fallback to symbol */}
                {data.name || data.symbol}
            </div>
            {/* Column 2: Last Traded Price (LTP) */}
            <div className="stock-cell stock-detail">
                {/* Use formatValue for consistent price display (handles '--' placeholders) */}
                {formatValue(data.ltp ?? data.last_price)} {/* Use ltp if available, fallback to last_price */}
            </div>
            {/* Column 3: Change */}
            <div
                className="stock-cell stock-detail"
                style={{ color: getChangeColor(data.change) }} // Apply dynamic color based on change
            >
                {formatChange(data.change)} {/* Format the change value */}
            </div>
            {/* Column 4: Percentage Change */}
            <div
                className="stock-cell stock-detail"
                style={{ color: getChangeColor(data.change_percent) }} // Apply dynamic color based on change
            >
                {formatChangePercent(data.change_percent)} {/* Format the percentage change value */}
            </div>
        </div>
    );
};

// Use React.memo to optimize performance by preventing unnecessary re-renders
// if the props (data, isSelected) haven't changed.
export default React.memo(Stock);
