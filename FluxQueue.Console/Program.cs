using Amqp;
using Amqp.Framing;
using static System.Runtime.InteropServices.JavaScript.JSType;
using System.Net;

var address = new Address("amqp://localhost:5672");
var connection = await Connection.Factory.CreateAsync(address);
var session = new Session(connection);

// SEND
var sender = new SenderLink(session, "sender-link", "orders");

var message = new Message
{
    BodySection = new Data { Binary = System.Text.Encoding.UTF8.GetBytes("hello amqp") }
};

await sender.SendAsync(message);
Console.WriteLine("Message sent");

// RECEIVE
var receiver = new ReceiverLink(session, "receiver-link", "orders");
receiver.SetCredit(1, false);

var received = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
if (received != null)
{
    Console.WriteLine("Received: " +
        System.Text.Encoding.UTF8.GetString(((Data)received.BodySection).Binary));

    receiver.Accept(received); // triggers AckAsync
    Console.WriteLine("Message received");
}

await receiver.CloseAsync();
await sender.CloseAsync();
await session.CloseAsync();
await connection.CloseAsync();