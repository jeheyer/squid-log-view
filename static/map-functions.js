var map;
var bounds;

function initMap() {

    // Create new map & bounds
    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 10,
        mapTypeId: google.maps.MapTypeId.ROADMAP
    });

    bounds  = new google.maps.LatLngBounds();

}

function PlotMarker(location) {

    // The 8 known Google maps marker colors
    let markercolors = ['blue', 'red', 'yellow', 'lightblue', 'orange', 'green', 'purple', 'pink'];

    // Use this to randomize colos
    let colorindex = Math.floor(Math.random() * markercolors.length);
    let icon_url = `https://maps.gstatic.com/mapfiles/ms2/micons/${markercolors[colorindex]}.png`;
    //let icon_url = `static/mapicons/${markercolors[colorindex]}.png`;

    // Create the marker
    let marker = new google.maps.Marker({
        position: new google.maps.LatLng(location.lat, location.lng),
        icon: { url: icon_url },
        title: location.title,
        map: map,
        draggable: false
    });

    // Plot the location
    let loc = new google.maps.LatLng(marker.position.lat(), marker.position.lng());
    console.log(`Plotted location '${marker.title}': ${marker.position.lat()}, ${marker.position.lng()}`);

    // Re-zoom based on location
    bounds.extend(loc);

    // Add marker detailed information when clicked
    if (typeof location.details !== 'undefined' ) {
        let infowindow = new google.maps.InfoWindow({});
        google.maps.event.addListener(marker, 'click', (function (marker) {
            return function () {
                infowindow.setContent(location.details);
                infowindow.open(map, marker);
            }
        })(marker));
    }

}

let successCallBack = (position)=>{

    locations[0].lat = position.coords.latitude;
    locations[0].lng = position.coords.longitude;

    // Create the Marker
    let marker = new google.maps.Marker({
        position: new google.maps.LatLng(locations[0].lat, locations[0].lng),
        // This icon hosted at J5 because I can't find the Google-hosted one
        icon: { url: "https://code.j5.org/static/mapicons/mylocation.png" },
        title: locations[0].title,
        map: map,
        draggable: false
    });

    // Plot the location
    let loc = new google.maps.LatLng(marker.position.lat(), marker.position.lng());

    // Adjust boundaries and zoom
    bounds.extend(loc);
    map.fitBounds(bounds);
    map.panToBounds(bounds);

    // Center around user's location, if desired
    //map.setCenter({lat: locations[0].lat, lng: locations[0].lng});

    console.log("Located user! lat: "+ locations[0].lat +", lng: "+ locations[0].lng);

};

let errorCallBack = (positionError)=>{

     console.log("Error locating user.  Message: " + positionError.message);

};

function locateMe () {

     window.navigator.geolocation.getCurrentPosition(successCallBack, errorCallBack);

}

