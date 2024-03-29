﻿namespace SharpPulsar_Examples
{
    /// <summary>
    /// An example runner that handles command line parsing.
    /// </summary>
    public abstract class ExampleRunner<T> where T : PulsarClientFlags
	{
		protected internal abstract string Name();

		protected internal abstract string Description();

		protected internal abstract T Flags();

	}

}
