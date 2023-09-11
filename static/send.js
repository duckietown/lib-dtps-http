const button = document.getElementById('myButton');
const textarea = document.getElementById('myTextArea');
const textarea_content_type = document.getElementById('myTextAreaContentType');


if (button !== null) {
    button.addEventListener('click', () => {
        const content_type = textarea_content_type.value;
        const content_json = jsyaml.load(textarea.value);
        let data;
        if (content_type === "application/json") {
            data = JSON.stringify(content_json);
        } else if (content_type === "application/cbor") {
            data = CBOR.encode(content_json);
        } else if (content_type === "application/yaml") {
            data = jsyaml.dump(content_json);

        } else {
            alert("Unknown content type: " + content_type);
            return;
        }
        // const content_cbor = CBOR.encode(content_json);

        fetch('.', {
            method: 'POST',
            headers: {'Content-Type': content_type},
            body: data
        })
            .then(handle_response)
            .catch(error => console.error('Error:', error));
    });
}

async function handle_response(r) {
    // r is a promise
    // await it
    if (r.ok) {
        console.log("ok");

    } else {
        console.error(r.statusText);
        // write the texst
        let text = await r.text();

        console.error(text);

    }

}

function subscribeWebSocket(url, fieldId) {
    // Initialize a new WebSocket connection
    let socket = new WebSocket(url);

    // Connection opened
    socket.addEventListener('open', function (event) {
        console.log('WebSocket connection established');
       let field = document.getElementById(fieldId);

        if (field) {
            field.textContent = 'WebSocket connection established';
        }
    });

    // Listen for messages
    socket.addEventListener('message', async function (event) {
        console.log('Message from server: ', event);
        let message0 = await convert(event);

        if ('DataReady' in message0) {
            let dr = message0['DataReady']

            let now = (performance.now() + performance.timeOrigin) * 1000.0* 1000.0;
            let diff = now - dr.time_inserted;

            let diff_ms = diff / 1000.0 / 1000.0;
            // console.log("diff", now, dr.time_inserted, diff);

            let s = "Received this notification with " + diff_ms.toFixed(3) + " ms latency:\n";
            // console.log('Message from server: ', message);

            // Find the field by ID and update its content
            let field = document.getElementById(fieldId);
            if (field) {
                // field.textContent = s + JSON.stringify(message0, null, 4);
                field.textContent = s + jsyaml.dump(message0);
            }
        } else if ('ChannelInfo' in message0) {
            console.log("ChannelInfo", message0);
            let field = document.getElementById(fieldId);
            if (field) {
                field.textContent = jsyaml.dump(message0);
            }

        } else {
            console.log("unknown message", message0);
        }
    });

    // Connection closed
    socket.addEventListener('close', function (event) {
        console.log('WebSocket connection closed');
         let field = document.getElementById(fieldId);
        if (field) {
            field.textContent = field.textContent + '\nWebSocket connection CLOSED';
        }
    });

    // Connection error
    socket.addEventListener('error', function (event) {
        console.error('WebSocket error: ', event);
           let field = document.getElementById(fieldId);
        if (field) {
            field.textContent = 'WebSocket error';
        }
    });
}

async function convert(event) {
    if (event.data instanceof ArrayBuffer) {
        // The data is an ArrayBuffer - decode it as CBOR
        return CBOR.decode(event.data);
    } else if (event.data instanceof Blob) {
        try {
            const arrayBuffer = await readFileAsArrayBuffer(event.data);
            return CBOR.decode(arrayBuffer);
        } catch (error) {
            console.error('Error reading blob: ', error);
            return  {'Error': error};
        }
    } else {
        console.error('Unknown data type: ', event.data);
        return  {'Unknown data type': event.data};
    }

}

function readFileAsArrayBuffer(blob) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();

        reader.onloadend = () => resolve(reader.result);
        reader.onerror = reject;

        reader.readAsArrayBuffer(blob);
    });
}


document.addEventListener("DOMContentLoaded", function () {
    let s = ((window.location.protocol === "https:") ? "wss://" : "ws://") + window.location.host + window.location.pathname + ":events";

    console.log("subscribing to: ", s);

    subscribeWebSocket(s, 'result');
});
