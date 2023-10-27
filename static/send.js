document.addEventListener("DOMContentLoaded", function () {

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
    } else {
        console.log("no button found");
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


});

function subscribeWebSocket(url, fieldId, data_field, data_field_image) {
    // Initialize a new WebSocket connection
    let socket = new WebSocket(url);
    let field = document.getElementById(fieldId);

    let base_url = url.replace("wss://", "https://").replace("ws://", "http://");


    // Connection opened
    socket.addEventListener('open', function (event) {
        console.log('WebSocket connection established', event);
        let field = document.getElementById(fieldId);
        if (field) {
            field.textContent = 'WebSocket connection established';
        }
    });

    let i = 0;
    let ndownloads_active = 0;
    // Listen for messages
    socket.addEventListener('message', async function (event) {
        // console.log('Message from server: ', event);
        // Find the field by ID and update its content

        let message0 = await convert(event);
        // console.log('Message from server: ', message0);


        if ('DataReady' in message0) {
            let dr = message0['DataReady']

            let now = (performance.now() + performance.timeOrigin) * 1000.0 * 1000.0;
            let diff = now - dr.time_inserted;

            let diff_ms = diff / 1000.0 / 1000.0;

            // console.log("diff", now, dr.time_inserted, diff);

            let availability = dr.availability[0].url;
            let use_url = new URL(availability, base_url);
            // console.log("ndownloads_active", ndownloads_active);
            // download from the url

            async function download(the_url) {
                let data = await fetch(the_url);
                let blob = await data.blob();
                let content_type = data.headers.get('Content-Type');

                let interpreted = await interpret_blob(blob, content_type);
                // console.log("interpreted", interpreted);
                // check if it is an image by checking the content type starting with "image"
                let is_image = content_type.indexOf('image') >= 0;
                if (is_image) {
                    // console.log("is image", interpreted);
                    let img_field_ = document.getElementById(data_field_image);
                    if (img_field_) {
                        img_field_.src = URL.createObjectURL(blob);
                    }

                } else {
                    let data_field_ = document.getElementById(data_field);
                    if (i > 0) {
                        if (data_field_) {
                            data_field_.textContent = jsyaml.dump(interpreted);
                        }
                    }
                }
            }

            // if (i > 0 && diff_ms > 500) {
            //     console.log("skipping download, too old", diff_ms, 'ndownloads_active', ndownloads_active);
            //     return;
            // }

             let s = "Received this notification with " + diff_ms.toFixed(3) + " ms latency:\n\n";
            // console.log('Message from server: ', message);

            if (field) {
                // field.textContent = s + JSON.stringify(message0, null, 4);
                field.textContent = s + jsyaml.dump(message0);
            }

                ndownloads_active += 1;
                download(use_url).then(r => ndownloads_active -= 1);


            i += 1;

        } else if ('ChannelInfo' in message0) {
            // console.log("ChannelInfo", message0);
            let field = document.getElementById(fieldId);
            if (field) {
                field.textContent = jsyaml.dump(message0);
            }

        } else {
            console.log("unknown message", message0);
            let field = document.getElementById(fieldId);
            if (field) {
                field.textContent = jsyaml.dump(message0);
            }

        }
    });

    // Connection closed
    socket.addEventListener('close', function (event) {
        console.log('WebSocket connection closed', event);
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
            return {'Error': error};
        }
    } else {
        console.error('Unknown data type: ', event.data);
        return {'Unknown data type': event.data};
    }

}

async function interpret_blob(data, content_type) {
    if (content_type.indexOf('cbor') >= 0) {
        let content = await data.arrayBuffer();
        return CBOR.decode(content);
    } else if (content_type.indexOf('json') >= 0) {
        let content = await data.text();
        return JSON.parse(content);
    } else if (content_type.indexOf('yaml') >= 0) {
        let content = await data.text();
        return jsyaml.load(content)
    } else if (content_type.indexOf('text') >= 0) {
        return await data.text();
    } else {
        return content_type + ' ' + data.toString();
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
    let s = ((window.location.protocol === "https:") ? "wss://" : "ws://") + window.location.host + window.location.pathname + ":events/";

    console.log("subscribing to: ", s);

    subscribeWebSocket(s, 'result', 'data_field', 'data_field_image');
});
