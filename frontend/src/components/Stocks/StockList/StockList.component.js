import React, { useEffect, useRef } from 'react';
import { HEADERS, STOCK_DATA } from '../../../share/utils/constant';
import $ from 'jquery';
import 'jquery-ui/ui/widgets/resizable';
import 'jquery-ui/ui/widgets/selectable';
import './StockList.component.scss';
import Stock from '../Stock/Stock.component';



const StockList = () => {
  const headerRef = useRef(null);
  const listRef = useRef(null);

  useEffect(() => {
    if (headerRef.current) {
      const $headerCells = $(headerRef.current).find('.stock-cell');

      $headerCells.each(function (index) {
        const $cell = $(this);
        const initialWidth = $cell.width();

        $('.stock-row').each(function () {
          $(this).children().eq(index).width(initialWidth);
        });

        $cell.resizable({
          handles: 'e',
          minWidth: 80,
          resize: function (event, ui) {
            const newWidth = ui.size.width;
            $('.stock-row').each(function () {
              $(this).children().eq(index).width(newWidth);
            });
          }
        });
      });
    }

    const $listRef = $(listRef.current);
    if ($listRef.length) {
      $listRef.selectable({
        filter: '.stock-row',
        selected: function (event, ui) {
          $(ui.selected).addClass('selected');
        },
        unselected: function (event, ui) {
          $(ui.unselected).removeClass('selected');
        }
      });
    }
  }, []);

  return (
    <div className="stock-menu">
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

      <div className="selectable" ref={listRef}>
        {STOCK_DATA.map((data, index) => (
          <Stock key={index} data={data} />
        ))}
      </div>
    </div>
  );
};

export default StockList;
