export class BroadcastTransport {
  constructor(channelName = 'shared-counter') {
    this.channel = new BroadcastChannel(channelName);
  }

  subscribe(handleMessage) {
    this.channel.onmessage = (event) => handleMessage(event.data);
  }

  send(message) {
    this.channel.postMessage(message);
  }

  close() {
    this.channel.close();
  }
}
