using SharpPulsar.Common;
using SharpPulsar.Protocol.Proto;

/// <summary>
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
/// 
///     http://www.apache.org/licenses/LICENSE-2.0
/// 
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
/// </summary>
namespace SharpPulsar_Examples
{
	using SubscriptionInitialPosition = SubscriptionInitialPosition;
	using SubscriptionType = CommandSubscribe.SubType;
	using AckType = CommandAck.AckType;

	/// <summary>
	/// Common flags for a consumer example.
	/// </summary>
	public class ConsumerFlags : TopicFlags
	{
		public string subscriptionName = "test-sub";

		public SubscriptionType subscriptionType = CommandSubscribe.SubType.Exclusive;


		public SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Earliest;


		public int ackEveryNMessages = 1;
		public AckType? ackType = null;

	}
}
