function PopulateDropDown(element_id, options, selectedOption) {

  const selector = document.getElementById(element_id);
  selector.options.length = 1;

  if (Array.isArray(options)) {

    options.forEach(function (value, i) {
      let option = document.createElement("option");
      option.value = value;
      option.text = value;
      selector.add(option);
      if (value == selectedOption)
        document.getElementById(element_id).selectedIndex = i+1;
    });

  } else {

    for (const [key, value] of Object.entries(options)) {
      let option = document.createElement("option");
      option.value = key;
      option.text = key;
      selector.add(option);
    }

  }

}

function FillTable(div_id, data) {

  const locale = navigator.language;

      // Build a list of colums
      let columns = [];
      for (const key in data[0]) {
        if (columns.indexOf(key) === -1) {
          columns.push(key);
        }
      }

      // Fill Table Header
      let table = document.createElement("table");
      let tr = table.insertRow(-1); 
      for (let i = 0; i < columns.length; i++) {
        let th = document.createElement("th");
        th.innerHTML = columns[i];
        tr.appendChild(th);
      }

      // Fill the rows
      for (let i = 0; i < data.length; i++) {
        tr = table.insertRow(-1);
        //console.log(data[i])
        for (let j = 0; j < columns.length; j++) {
          let tabCell = tr.insertCell(-1);
          if (j==0) {
            const timestamp = data[i][columns[j]] * 1000;
            const date = new Date(timestamp).toLocaleDateString(locale);
            const time = new Date(timestamp).toLocaleTimeString(locale);
            tabCell.innerHTML = `${time} ${date}`;
          } else {
            tabCell.innerHTML = data[i][columns[j]];
          }
        }
        tr.className = (i % 2  == 0) ? "even" : "odd";
      }
      var divContainer = document.getElementById(div_id);
      divContainer.innerHTML = "";
      divContainer.appendChild(table);

}
