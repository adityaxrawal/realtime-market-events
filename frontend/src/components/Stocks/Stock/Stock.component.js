import React from 'react';

const getChangeColor = (value) => {
  return value > 0 ? 'green' : value < 0 ? '#E63644' : '#23A18C';
};

const Stock = ({ data }) => {
  return (
    <div className="stock-row">
      <div className="stock-cell symbol" title={data.symbol}>{data.name}</div>
      <div className="stock-cell stock-detail">{data.last_price.toFixed(2)}</div>
      <div className="stock-cell stock-detail" style={{ color: getChangeColor(data.change) }}>
        {data.change > 0 ? `+${data.change}` : data.change}
      </div>
      <div className="stock-cell stock-detail" style={{ color: getChangeColor(data.change_percent) }}>
        {data.change_percent > 0 ? `+${data.change_percent}%` : `${data.change_percent}%`}
      </div>
    </div>
  );
};

export default React.memo(Stock); // optimization
